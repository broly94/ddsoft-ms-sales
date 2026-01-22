from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Body
from fastapi.responses import JSONResponse, StreamingResponse
import pandas as pd
import io
from src.core.route_validator.service import RouteValidator
from src.models.route_models import save_recorrido_to_db, get_recorrido_from_db, save_resultados_to_db, get_historial_validaciones, save_lista_resultados_to_db

router = APIRouter(prefix="/route-validator", tags=["Route Validator"])

@router.get("/history")
async def get_history():
    try:
        df_history = get_historial_validaciones()
        return df_history.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/recorrido/upload")
async def upload_recorrido(file: UploadFile = File(...)):
    try:
        content = await file.read()
        df_prog = RouteValidator.procesar_recorrido(content)
        if df_prog.empty:
            raise HTTPException(status_code=400, detail="El archivo no contiene datos válidos.")
        
        save_recorrido_to_db(df_prog)
        return {"message": "Archivo de recorrido cargado y guardado en base de datos correctamente.", "filas": len(df_prog)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/validate")
async def validate_routes(
    recorrido: UploadFile = File(None),
    horario: UploadFile = File(...),
    fecha_inicio: str = Form("01/10/2025"),
    fecha_fin: str = Form("30/11/2025"),
    semana_inicio: int = Form(1), # 1 o 2
    format: str = Form("json")
):
    try:
        horario_content = await horario.read()
        if recorrido:
            recorrido_content = await recorrido.read()
            df_res, df_horas, df_prog = RouteValidator.procesar_datos(
                recorrido_content, 
                horario_content, 
                fecha_inicio, 
                fecha_fin
            )
        else:
            df_prog_db = get_recorrido_from_db()
            if df_prog_db.empty:
                raise HTTPException(status_code=400, detail="No hay datos de recorrido en la base de datos.")
            
            df_res, df_horas, df_prog = RouteValidator.procesar_con_df_prog(
                df_prog_db,
                horario_content,
                fecha_inicio,
                fecha_fin,
                semana_inicio=semana_inicio
            )

        if df_res.empty:
            return JSONResponse(status_code=404, content={"message": "No se encontraron coincidencias."})
        
        if format == "excel":
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_res.to_excel(writer, sheet_name='Visitas_Correctas', index=False)
                df_horas.to_excel(writer, sheet_name='Horas', index=False)
                df_prog.to_excel(writer, sheet_name='Base_Ruta_Desglosada', index=False)
            output.seek(0)
            return StreamingResponse(
                output,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": "attachment; filename=Reporte_Ruta.xlsx"}
            )
        
        # Formateo uniforme de fechas para JSON
        df_res_json = df_res.copy()
        for col in ['Fecha_Checkin', 'Fecha_Checkout']:
            if col in df_res_json.columns:
                df_res_json[col] = pd.to_datetime(df_res_json[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        return {
            "visitas": df_res_json.to_dict(orient="records"),
            "horas_vendedor": df_horas.to_dict(orient="records"),
            "resumen": {
                "total_visitas": len(df_res),
                "total_vendedores": len(df_horas)
            }
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@router.post("/save-batch")
async def save_batch(visitas: list = Body(..., embed=True)):
    try:
        if not visitas:
            raise HTTPException(status_code=400, detail="La lista de visitas está vacía.")
        
        batch_id = save_lista_resultados_to_db(visitas)
        return {"message": "Procesamiento guardado exitosamente.", "batch_id": batch_id, "filas": len(visitas)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
