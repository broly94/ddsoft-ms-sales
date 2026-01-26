from datetime import datetime, timedelta
import pandas as pd
from src.models.route_models import (
    SessionLocal, AxumGpsModel, FrecuenciaHeaderModel, FrecuenciaModel, 
    HorasHeaderModel, HorasDetalleModel, ViaticoConfigModel
)

class FrecuenciaRepository:
    @staticmethod
    def save_batch(visitas: list, horas_resumen: list, horas_detalle: list, metadata: dict):
        db = SessionLocal()
        try:
            batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            
            # 1. Parse dates from metadata
            f_desde = metadata.get('fecha_desde')
            if isinstance(f_desde, str):
                try: f_desde = datetime.strptime(f_desde, "%d/%m/%Y")
                except: f_desde = pd.to_datetime(f_desde)
                
            f_hasta = metadata.get('fecha_hasta')
            if isinstance(f_hasta, str):
                try: f_hasta = datetime.strptime(f_hasta, "%d/%m/%Y")
                except: f_hasta = pd.to_datetime(f_hasta)

            # 2. AxumGPS (ex-frecuencia_header, metadata del procesamiento)
            axum_gps = AxumGpsModel(
                batch_id=batch_id,
                fecha_desde=f_desde,
                fecha_hasta=f_hasta
            )
            db.add(axum_gps)
            db.flush()
            
            # 3. Frecuencia (Visitas detalladas)
            for item in visitas:
                f_checkin = item.get('Fecha_Checkin')
                if isinstance(f_checkin, str):
                    f_checkin = datetime.strptime(f_checkin, '%Y-%m-%d %H:%M:%S')
                
                f_checkout = item.get('Fecha_Checkout')
                if isinstance(f_checkout, str):
                    try: f_checkout = datetime.strptime(f_checkout, '%Y-%m-%d %H:%M:%S')
                    except: f_checkout = pd.to_datetime(f_checkout)

                db_frec = FrecuenciaModel(
                    axum_gps_id=axum_gps.id,
                    batch_id=batch_id,
                    vendedor=item.get('Vendedor'),
                    cliente=item.get('Cliente'),
                    fecha_checkin=f_checkin,
                    fecha_checkout=f_checkout,
                    tiempo_pdv_original=item.get('Tiempo_PDV_Original'),
                    tiempo_pdv_limitado=item.get('Tiempo_PDV_Limitado'),
                    dia_real=item.get('Dia_Real'),
                    semana_real=item.get('Semana_Real'),
                    programacion=item.get('Programacion'),
                    bloque=item.get('Bloque'),
                    linea=item.get('Linea'),
                    estado=item.get('Estado')
                )
                db.add(db_frec)
            
            # 4. Calcular Frecuencia_Header (Agregación por vendedor)
            # Agrupar visitas por vendedor y sumar tiempo_pdv_original
            vendedor_totals = {}
            for item in visitas:
                vendedor = item.get('Vendedor')
                tiempo_str = item.get('Tiempo_PDV_Original', '00:00:00')
                
                # Convertir tiempo a timedelta para sumar
                try:
                    parts = tiempo_str.split(':')
                    if len(parts) == 3:
                        td = timedelta(hours=int(parts[0]), minutes=int(parts[1]), seconds=int(parts[2]))
                    else:
                        td = timedelta(0)
                except:
                    td = timedelta(0)
                
                if vendedor not in vendedor_totals:
                    vendedor_totals[vendedor] = timedelta(0)
                vendedor_totals[vendedor] += td
            
            # Guardar agregaciones
            for vendedor, total_td in vendedor_totals.items():
                total_seconds = int(total_td.total_seconds())
                tiempo_total_str = f"{total_seconds//3600:02}:{(total_seconds%3600)//60:02}:{total_seconds%60:02}"
                
                db_frec_header = FrecuenciaHeaderModel(
                    axum_gps_id=axum_gps.id,
                    vendedor=vendedor,
                    tiempo_pdv_total=tiempo_total_str
                )
                db.add(db_frec_header)
                
            # 5. Horas Resumen (horas_header)
            for h in horas_resumen:
                db_hora_header = HorasHeaderModel(
                    axum_gps_id=axum_gps.id,
                    vendedor=h.get('Vendedor'),
                    horas_totales=h.get('Horas_Totales'),
                    dias_trabajados=h.get('Dias_Trabajados'),
                    promedio_horas_diarias=h.get('Promedio_Horas'),
                    promedio_checkin=h.get('Promedio_Checkin'),
                    promedio_checkout=h.get('Promedio_Checkout'),
                    viatico=h.get('Viatico', 0.0),
                    linea=h.get('Linea', '')
                )
                db.add(db_hora_header)
            
            # 6. Horas Detalle Diario (horas)
            for detalle in horas_detalle:
                # Parse fecha
                fecha_dt = detalle.get('fecha')
                if isinstance(fecha_dt, str):
                    try: fecha_dt = datetime.strptime(fecha_dt, '%Y-%m-%d')
                    except: fecha_dt = pd.to_datetime(fecha_dt)
                
                # Parse primer_checkin
                primer_checkin_dt = detalle.get('primer_checkin')
                if isinstance(primer_checkin_dt, str):
                    try: primer_checkin_dt = datetime.strptime(primer_checkin_dt, '%Y-%m-%d %H:%M:%S')
                    except: primer_checkin_dt = pd.to_datetime(primer_checkin_dt)
                
                # Parse ultimo_checkout
                ultimo_checkout_dt = detalle.get('ultimo_checkout')
                if isinstance(ultimo_checkout_dt, str):
                    try: ultimo_checkout_dt = datetime.strptime(ultimo_checkout_dt, '%Y-%m-%d %H:%M:%S')
                    except: ultimo_checkout_dt = pd.to_datetime(ultimo_checkout_dt)
                
                db_hora_detalle = HorasDetalleModel(
                    axum_gps_id=axum_gps.id,
                    vendedor=detalle.get('vendedor'),
                    cliente=detalle.get('cliente'),
                    fecha=fecha_dt,
                    primer_checkin=primer_checkin_dt,
                    ultimo_checkout=ultimo_checkout_dt,
                    total_horas_dia=detalle.get('total_horas_dia')
                )
                db.add(db_hora_detalle)
                
            db.commit()
            return batch_id
        except Exception as e:
            db.rollback()
            raise e
        finally:
            db.close()

    @staticmethod
    def get_history():
        db = SessionLocal()
        try:
            items = db.query(AxumGpsModel).order_by(AxumGpsModel.fecha_proceso.desc()).all()
            return [{
                "id": item.id,
                "batch_id": item.batch_id,
                "fecha_desde": item.fecha_desde.strftime("%Y-%m-%d") if item.fecha_desde else None,
                "fecha_hasta": item.fecha_hasta.strftime("%Y-%m-%d") if item.fecha_hasta else None,
                "fecha_proceso": item.fecha_proceso.strftime("%Y-%m-%d %H:%M:%S")
            } for item in items]
        finally:
            db.close()

    @staticmethod
    def get_viatico_configs():
        db = SessionLocal()
        try:
            return db.query(ViaticoConfigModel).all()
        finally:
            db.close()

    @staticmethod
    def update_viatico_config(zona, valor):
        db = SessionLocal()
        try:
            config = db.query(ViaticoConfigModel).filter(ViaticoConfigModel.zona == zona).first()
            if config:
                config.valor = valor
                db.commit()
                return True
            return False
        finally:
            db.close()
            
    @staticmethod
    def get_batch_details(batch_id: str):
        db = SessionLocal()
        try:
            axum_gps = db.query(AxumGpsModel).filter(AxumGpsModel.batch_id == batch_id).first()
            if not axum_gps: return None
            
            visitas = db.query(FrecuenciaModel).filter(FrecuenciaModel.axum_gps_id == axum_gps.id).all()
            frecuencia_headers = db.query(FrecuenciaHeaderModel).filter(FrecuenciaHeaderModel.axum_gps_id == axum_gps.id).all()
            horas_resumen = db.query(HorasHeaderModel).filter(HorasHeaderModel.axum_gps_id == axum_gps.id).all()
            horas_detalle = db.query(HorasDetalleModel).filter(HorasDetalleModel.axum_gps_id == axum_gps.id).all()
            
            return {
                "axum_gps": {
                    "batch_id": axum_gps.batch_id,
                    "fecha_desde": axum_gps.fecha_desde,
                    "fecha_hasta": axum_gps.fecha_hasta,
                },
                "visitas": [v.__dict__ for v in visitas],
                "frecuencia_headers": [{"vendedor": f.vendedor, "tiempo_pdv_total": f.tiempo_pdv_total} for f in frecuencia_headers],
                "horas_resumen": [h.__dict__ for h in horas_resumen],
                "horas_detalle": [h.__dict__ for h in horas_detalle]
            }
        finally:
            db.close()
