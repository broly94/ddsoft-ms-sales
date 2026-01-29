from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Body
from fastapi.responses import JSONResponse, StreamingResponse
import pandas as pd
import io

from src.services.recorrido_service import RecorridoService
from src.services.frecuencia_service import FrecuenciaService
from src.services.validator_service import ValidatorService

router = APIRouter(prefix="/route-validator", tags=["Route Validator"])

@router.get("/history")
async def get_history():
    try:
        return FrecuenciaService.get_history()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/history/{batch_id}")
async def get_batch_details(batch_id: str):
    try:
        result = FrecuenciaService.get_batch_details(batch_id)
        if not result:
            raise HTTPException(status_code=404, detail="Batch no encontrado")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/history/{batch_id}/hours")
async def get_batch_hours(batch_id: str):
    try:
        result = FrecuenciaService.get_batch_hours(batch_id)
        if not result:
            raise HTTPException(status_code=404, detail="Batch no encontrado")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/frecuencia/recent")
async def get_recent_frecuencia(
    limit: int = 50, 
    vendedor: str = None, 
    cliente: str = None,
    batch_id: str = None
):
    """
    Obtiene las últimas N visitas de frecuencia con filtros opcionales.
    - limit: Cantidad de registros a retornar (default: 50)
    - vendedor: Filtro por vendedor (búsqueda parcial)
    - cliente: Filtro por cliente (búsqueda parcial)
    - batch_id: Filtro por batch específico
    """
    try:
        return FrecuenciaService.get_recent_frecuencia(limit, vendedor, cliente, batch_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/frecuencia/summary")
async def get_frecuencia_summary(
    vendedor: str = None,
    batch_id: str = None
):
    """
    Obtiene el resumen de tiempos totales por vendedor.
    """
    try:
        return FrecuenciaService.get_frecuencia_summary(vendedor, batch_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/recorrido/upload")
async def upload_recorrido(file: UploadFile = File(...)):
    try:
        content = await file.read()
        df_prog = RecorridoService.upload_recorrido(content)
        if df_prog.empty:
            raise HTTPException(status_code=400, detail="El archivo no contiene datos válidos.")
        return {"message": "Archivo de recorrido cargado y guardado correctamente.", "filas": len(df_prog)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/recorrido")
async def list_recorridos(limit: int = 100, offset: int = 0, search_vendedor: str = None, 
                          search_cliente: str = None, linea: str = None, bloque: str = None,
                          semana: int = None, dia: str = None):
    try:
        items, total = RecorridoService.get_all_recorridos(
            limit=limit, offset=offset, vendedor=search_vendedor, 
            cliente=search_cliente, linea=linea, bloque=bloque, 
            semana=semana, dia=dia
        )
        return {"items": items, "total": total}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/recorrido/{id}")
async def get_recorrido(id: int):
    item = RecorridoService.get_recorrido_by_id(id)
    if not item: raise HTTPException(status_code=404, detail="Recorrido no encontrado")
    return item

@router.post("/recorrido")
async def add_recorrido(data: dict = Body(...)):
    try: return RecorridoService.create_recorrido(data)
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@router.put("/recorrido/{id}")
async def edit_recorrido(id: int, data: dict = Body(...)):
    try:
        item = RecorridoService.update_recorrido(id, data)
        if not item: raise HTTPException(status_code=404, detail="Recorrido no encontrado")
        return item
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@router.delete("/recorrido/{id}")
async def remove_recorrido(id: int):
    try:
        success = RecorridoService.delete_recorrido(id)
        if not success: raise HTTPException(status_code=404, detail="Recorrido no encontrado")
        return {"message": "Recorrido eliminado correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@router.post("/validate")
async def validate_routes(recorrido: UploadFile = File(None), horario: UploadFile = File(...),
                          fecha_inicio: str = Form("01/10/2025"), fecha_fin: str = Form("30/11/2025"),
                          semana_inicio: int = Form(1), format: str = Form("json")):
    try:
        horario_content = await horario.read()
        if recorrido:
            recorrido_content = await recorrido.read()
            df_prog = ValidatorService.procesar_recorrido(recorrido_content)
        else:
            df_prog = RecorridoService.get_recorrido_from_db()
            if df_prog.empty: raise HTTPException(status_code=400, detail="No hay datos de recorrido.")

        df_res, df_horas_resumen, df_horas_detalle, df_prog_full = ValidatorService.procesar_con_df_prog(
            df_prog, horario_content, fecha_inicio, fecha_fin, semana_inicio=semana_inicio
        )

        if df_res.empty:
            return JSONResponse(status_code=404, content={"message": "No se encontraron coincidencias."})
        
        if format == "excel":
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_res.to_excel(writer, sheet_name='Visitas', index=False)
                df_horas_resumen.to_excel(writer, sheet_name='Horas_Resumen', index=False)
                df_horas_detalle.to_excel(writer, sheet_name='Horas_Detalle', index=False)
            output.seek(0)
            return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                     headers={"Content-Disposition": "attachment; filename=Reporte.xlsx"})
        
        df_res_json = df_res.copy()
        for col in ['Fecha_Checkin', 'Fecha_Checkout']:
            if col in df_res_json.columns:
                df_res_json[col] = pd.to_datetime(df_res_json[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
        
        return {
            "metadata": {"fecha_desde": fecha_inicio, "fecha_hasta": fecha_fin},
            "visitas": df_res_json.to_dict(orient="records"),
            "horas_resumen": df_horas_resumen.to_dict(orient="records"),
            "horas_detalle": df_horas_detalle.to_dict(orient="records"),
            "resumen": {"total_visitas": len(df_res), "total_vendedores": len(df_horas_resumen)}
        }
    except Exception as e:
        import traceback; print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/save-batch")
async def save_batch(visitas: list = Body(..., embed=True), 
                     horas_resumen: list = Body(..., embed=True),
                     horas_detalle: list = Body(..., embed=True),
                     metadata: dict = Body(..., embed=True)):
    try:
        batch_id = FrecuenciaService.process_and_save_batch(visitas, horas_resumen, horas_detalle, metadata)
        return {"message": "Procesamiento guardado exitosamente.", "batch_id": batch_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/viatico-config")
async def get_viatico_settings():
    return FrecuenciaService.get_viatico_configs()

@router.put("/viatico-config")
async def update_viatico_settings(zona: str = Body(...), valor: float = Body(...)):
    if FrecuenciaService.update_viatico_config(zona, valor):
        return {"message": f"Configuración de viático para {zona} actualizada."}
    raise HTTPException(status_code=404, detail="Zona no encontrada.")
