from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
import pandas as pd
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user_sales:password_sales@db_sales:5432/db_sales")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class RecorridoModel(Base):
    __tablename__ = "recorridos"
    id = Column(Integer, primary_key=True, index=True)
    clave = Column(String, index=True) # {cliente}_{vendedor}_{dia}_{semana}
    vendedor = Column(String)
    cliente = Column(String)
    dia_prog = Column(String)
    semana_prog = Column(Integer)
    bloque = Column(String)
    linea_origen = Column(String)
    texto_original = Column(String)
    fecha_carga = Column(DateTime, default=datetime.utcnow)

class VisitaValidadaModel(Base):
    __tablename__ = "visitas_validadas"
    id = Column(Integer, primary_key=True, index=True)
    batch_id = Column(String, index=True) # Para agrupar todas las visitas de una misma carga/dia
    vendedor = Column(String)
    cliente = Column(String)
    fecha_checkin = Column(DateTime)
    fecha_checkout = Column(DateTime)
    tiempo_pdv_original = Column(String)
    tiempo_pdv_limitado = Column(String)
    dia_real = Column(String)
    semana_real = Column(Integer)
    programacion = Column(String)
    bloque = Column(String)
    linea = Column(String)
    estado = Column(String)
    fecha_proceso = Column(DateTime, default=datetime.utcnow)

import time

def init_db():
    retries = 5
    while retries > 0:
        try:
            Base.metadata.create_all(bind=engine)
            print("Conexión a la base de datos exitosa.")
            break
        except Exception as e:
            retries -= 1
            print(f"Error conectando a la base de datos. Reintentando en 5 segundos... ({retries} intentos restantes)")
            time.sleep(5)
    if retries == 0:
        print("No se pudo conectar a la base de datos después de varios intentos.")

def save_recorrido_to_db(df_prog: pd.DataFrame):
    db = SessionLocal()
    try:
        # Limpiar tabla anterior (asumimos que solo hay un recorrido vigente o el usuario sube uno nuevo completo)
        db.query(RecorridoModel).delete()
        
        for _, row in df_prog.iterrows():
            db_item = RecorridoModel(
                clave=row['Clave'],
                vendedor=row['Vendedor'],
                cliente=row['Cliente'],
                dia_prog=row['Dia_Prog'],
                semana_prog=row['Semana_Prog'],
                bloque=row['Bloque'],
                linea_origen=row['Linea_Origen'],
                texto_original=row['Texto_Original']
            )
            db.add(db_item)
        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def get_recorrido_from_db():
    db = SessionLocal()
    try:
        items = db.query(RecorridoModel).all()
        if not items:
            return pd.DataFrame()
        
        data = []
        for item in items:
            data.append({
                'Clave': item.clave,
                'Vendedor': item.vendedor,
                'Cliente': item.cliente,
                'Dia_Prog': item.dia_prog,
                'Semana_Prog': item.semana_prog,
                'Bloque': item.bloque,
                'Linea_Origen': item.linea_origen,
                'Texto_Original': item.texto_original
            })
        return pd.DataFrame(data)
    finally:
        db.close()

def get_all_recorridos(
    limit: int = 100, 
    offset: int = 0, 
    search_vendedor: str = None,
    search_cliente: str = None,
    linea: str = None,
    bloque: str = None,
    semana: int = None,
    dia: str = None
):
    db = SessionLocal()
    try:
        query = db.query(RecorridoModel)
        
        if search_vendedor:
            query = query.filter(RecorridoModel.vendedor.ilike(f"%{search_vendedor}%"))
            
        if search_cliente:
            query = query.filter(RecorridoModel.cliente.ilike(f"%{search_cliente}%"))
            
        if linea and linea != "all":
            # linea_origen suele ser "Linea 1", "Linea 2", etc.
            query = query.filter(RecorridoModel.linea_origen.ilike(f"%{linea}%"))
            
        if bloque and bloque != "all":
            query = query.filter(RecorridoModel.bloque.ilike(f"%{bloque}%"))
            
        if semana and semana != 0:
            query = query.filter(RecorridoModel.semana_prog == semana)
            
        if dia and dia != "all":
            query = query.filter(RecorridoModel.dia_prog.ilike(f"{dia}"))
        
        total = query.count()
        items = query.order_by(RecorridoModel.id.desc()).offset(offset).limit(limit).all()
        
        return items, total
    finally:
        db.close()

def get_recorrido_by_id(recorrido_id: int):
    db = SessionLocal()
    try:
        return db.query(RecorridoModel).filter(RecorridoModel.id == recorrido_id).first()
    finally:
        db.close()

def create_recorrido(data: dict):
    db = SessionLocal()
    try:
        db_item = RecorridoModel(**data)
        db.add(db_item)
        db.commit()
        db.refresh(db_item)
        return db_item
    finally:
        db.close()

def update_recorrido(recorrido_id: int, data: dict):
    db = SessionLocal()
    try:
        db_item = db.query(RecorridoModel).filter(RecorridoModel.id == recorrido_id).first()
        if not db_item:
            return None
        for key, value in data.items():
            setattr(db_item, key, value)
        db.commit()
        db.refresh(db_item)
        return db_item
    finally:
        db.close()

def delete_recorrido(recorrido_id: int):
    db = SessionLocal()
    try:
        db_item = db.query(RecorridoModel).filter(RecorridoModel.id == recorrido_id).first()
        if not db_item:
            return False
        db.delete(db_item)
        db.commit()
        return True
    finally:
        db.close()

def save_resultados_to_db(df_res: pd.DataFrame):
    db = SessionLocal()
    try:
        batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        for _, row in df_res.iterrows():
            db_item = VisitaValidadaModel(
                batch_id=batch_id,
                vendedor=row['Vendedor'],
                cliente=row['Cliente'],
                fecha_checkin=row['Fecha_Checkin'],
                fecha_checkout=row['Fecha_Checkout'],
                tiempo_pdv_original=row['Tiempo_PDV_Original'],
                tiempo_pdv_limitado=row['Tiempo_PDV_Limitado'],
                dia_real=row['Dia_Real'],
                semana_real=row['Semana_Real'],
                programacion=row['Programacion'],
                bloque=row['Bloque'],
                linea=row['Linea'],
                estado=row['Estado']
            )
            db.add(db_item)
        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def get_historial_validaciones():
    db = SessionLocal()
    try:
        items = db.query(VisitaValidadaModel).order_by(VisitaValidadaModel.fecha_proceso.desc()).all()
        if not items:
            return pd.DataFrame()
        
        data = []
        for item in items:
            data.append({
                'ID': item.id,
                'Batch': item.batch_id,
                'Vendedor': item.vendedor,
                'Cliente': item.cliente,
                'Fecha_Checkin': item.fecha_checkin,
                'Estado': item.estado,
                'Fecha_Proceso': item.fecha_proceso
            })
        return pd.DataFrame(data)
    finally:
        db.close()
def save_lista_resultados_to_db(lista_visitas: list):
    db = SessionLocal()
    try:
        batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        for item in lista_visitas:
            # Convertir strings de fecha a objetos datetime si es necesario
            f_checkin = item.get('Fecha_Checkin')
            if isinstance(f_checkin, str):
                f_checkin = datetime.strptime(f_checkin, '%Y-%m-%d %H:%M:%S')
            
            f_checkout = item.get('Fecha_Checkout')
            if isinstance(f_checkout, str):
                try:
                    f_checkout = datetime.strptime(f_checkout, '%Y-%m-%d %H:%M:%S')
                except:
                    # Fallback por si viene en otro formato
                    f_checkout = pd.to_datetime(f_checkout)

            db_item = VisitaValidadaModel(
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
            db.add(db_item)
        db.commit()
        return batch_id
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()
