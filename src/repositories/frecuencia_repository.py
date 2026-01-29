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
            
            # Obtener semana_inicio (default 1 si no está definido)
            semana_inicio = metadata.get('semana_inicio', 1)

            # 2. AxumGPS (ex-frecuencia_header, metadata del procesamiento)
            axum_gps = AxumGpsModel(
                batch_id=batch_id,
                fecha_desde=f_desde,
                fecha_hasta=f_hasta,
                semana_inicio=semana_inicio
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
    
    @staticmethod
    def get_batch_details_with_cliente_count(batch_id: str):
        """Obtiene detalles de un batch incluyendo conteo de clientes únicos por vendedor (SOLO RESUMEN)"""
        db = SessionLocal()
        try:
            axum_gps = db.query(AxumGpsModel).filter(AxumGpsModel.batch_id == batch_id).first()
            if not axum_gps: return None
            
            # Obtener frecuencia_headers
            frecuencia_headers = db.query(FrecuenciaHeaderModel).filter(
                FrecuenciaHeaderModel.axum_gps_id == axum_gps.id
            ).all()
            
            # Contar clientes únicos por vendedor usando una query agregada (más eficiente)
            from sqlalchemy import func, distinct
            vendedor_stats = db.query(
                FrecuenciaModel.vendedor,
                func.count(distinct(FrecuenciaModel.cliente)).label('total_clientes')
            ).filter(
                FrecuenciaModel.axum_gps_id == axum_gps.id
            ).group_by(FrecuenciaModel.vendedor).all()
            
            # Crear un diccionario de vendedor -> total_clientes
            vendedor_clientes_count = {stat.vendedor: stat.total_clientes for stat in vendedor_stats}
            
            # Construir response con headers y conteo de clientes
            headers_with_count = []
            for header in frecuencia_headers:
                total_clientes = vendedor_clientes_count.get(header.vendedor, 0)
                headers_with_count.append({
                    "vendedor": header.vendedor,
                    "tiempo_pdv_total": header.tiempo_pdv_total,
                    "total_clientes": total_clientes
                })
            
            # NO cargamos todos los detalles de frecuencia para evitar cargar 25k+ registros
            # Retornamos solo metadata y el resumen
            return {
                "batch_id": axum_gps.batch_id,
                "fecha_desde": axum_gps.fecha_desde.strftime("%Y-%m-%d") if axum_gps.fecha_desde else None,
                "fecha_hasta": axum_gps.fecha_hasta.strftime("%Y-%m-%d") if axum_gps.fecha_hasta else None,
                "fecha_proceso": axum_gps.fecha_proceso.strftime("%Y-%m-%d %H:%M:%S") if axum_gps.fecha_proceso else None,
                "frecuencia_headers": headers_with_count,
                "total_visitas": db.query(FrecuenciaModel).filter(FrecuenciaModel.axum_gps_id == axum_gps.id).count()
            }
        finally:
            db.close()
    
    @staticmethod
    def get_batch_hours(batch_id: str):
        """Obtiene el resumen de horas (horas_header) de un batch específico"""
        db = SessionLocal()
        try:
            axum_gps = db.query(AxumGpsModel).filter(AxumGpsModel.batch_id == batch_id).first()
            if not axum_gps: return None
            
            # Obtener horas_header (resumen por vendedor)
            horas_header = db.query(HorasHeaderModel).filter(
                HorasHeaderModel.axum_gps_id == axum_gps.id
            ).all()
            
            # Construir response
            horas_data = []
            for hora in horas_header:
                horas_data.append({
                    "vendedor": hora.vendedor,
                    "horas_totales": hora.horas_totales,
                    "dias_trabajados": hora.dias_trabajados,
                    "promedio_horas_diarias": hora.promedio_horas_diarias,
                    "promedio_checkin": hora.promedio_checkin,
                    "promedio_checkout": hora.promedio_checkout,
                    "viatico": hora.viatico,
                    "linea": hora.linea
                })
            
            return {
                "batch_id": axum_gps.batch_id,
                "fecha_desde": axum_gps.fecha_desde.strftime("%Y-%m-%d") if axum_gps.fecha_desde else None,
                "fecha_hasta": axum_gps.fecha_hasta.strftime("%Y-%m-%d") if axum_gps.fecha_hasta else None,
                "fecha_proceso": axum_gps.fecha_proceso.strftime("%Y-%m-%d %H:%M:%S") if axum_gps.fecha_proceso else None,
                "horas_header": horas_data
            }
        finally:
            db.close()
    
    @staticmethod
    def get_recent_frecuencia(limit: int = 50, vendedor: str = None, cliente: str = None, batch_id: str = None):
        """Obtiene las últimas N visitas de frecuencia con filtros opcionales"""
        db = SessionLocal()
        try:
            query = db.query(FrecuenciaModel)
            
            # Aplicar filtros si se proporcionan
            if vendedor:
                query = query.filter(FrecuenciaModel.vendedor.ilike(f"%{vendedor}%"))
            if cliente:
                query = query.filter(FrecuenciaModel.cliente.ilike(f"%{cliente}%"))
            if batch_id:
                # Buscar el axum_gps_id correspondiente al batch_id
                axum_gps = db.query(AxumGpsModel).filter(AxumGpsModel.batch_id == batch_id).first()
                if axum_gps:
                    query = query.filter(FrecuenciaModel.axum_gps_id == axum_gps.id)
            
            # Ordenar por fecha más reciente y limitar
            visitas = query.order_by(FrecuenciaModel.fecha_checkin.desc()).limit(limit).all()
            
            # Construir response
            visitas_data = []
            for visita in visitas:
                visitas_data.append({
                    "id": visita.id,
                    "batch_id": visita.batch_id,
                    "vendedor": visita.vendedor,
                    "cliente": visita.cliente,
                    "fecha_checkin": visita.fecha_checkin.strftime("%Y-%m-%d %H:%M:%S") if visita.fecha_checkin else None,
                    "fecha_checkout": visita.fecha_checkout.strftime("%Y-%m-%d %H:%M:%S") if visita.fecha_checkout else None,
                    "tiempo_pdv_original": visita.tiempo_pdv_original,
                    "tiempo_pdv_limitado": visita.tiempo_pdv_limitado,
                    "dia_real": visita.dia_real,
                    "semana_real": visita.semana_real,
                    "programacion": visita.programacion,
                    "bloque": visita.bloque,
                    "linea": visita.linea,
                    "estado": visita.estado
                })
            
            return {
                "visitas": visitas_data,
                "total": len(visitas_data),
                "filtros_aplicados": {
                    "vendedor": vendedor,
                    "cliente": cliente,
                    "batch_id": batch_id,
                    "limit": limit
                }
            }
        finally:
            db.close()

    @staticmethod
    def get_frecuencia_summary(vendedor: str = None, batch_id: str = None):
        """Obtiene el resumen de tiempos totales por vendedor (desde FrecuenciaHeaderModel)"""
        db = SessionLocal()
        try:
            query = db.query(FrecuenciaHeaderModel)
            
            if batch_id:
                axum_gps = db.query(AxumGpsModel).filter(AxumGpsModel.batch_id == batch_id).first()
                if axum_gps:
                    query = query.filter(FrecuenciaHeaderModel.axum_gps_id == axum_gps.id)
            else:
                # Si no hay batch_id, buscar el último procesado
                last_gps = db.query(AxumGpsModel).order_by(AxumGpsModel.fecha_proceso.desc()).first()
                if last_gps:
                    query = query.filter(FrecuenciaHeaderModel.axum_gps_id == last_gps.id)
            
            if vendedor:
                query = query.filter(FrecuenciaHeaderModel.vendedor.ilike(f"%{vendedor}%"))
            
            headers = query.all()
            
            resumen = []
            for h in headers:
                resumen.append({
                    "vendedor": h.vendedor,
                    "tiempo_pdv_total": h.tiempo_pdv_total,
                    "batch_id": h.axum_gps.batch_id if h.axum_gps else batch_id
                })
            
            return resumen
        finally:
            db.close()

