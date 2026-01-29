from sqlalchemy import create_engine, Column, String, Integer, DateTime, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import os
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

class AxumGpsModel(Base):
    __tablename__ = "axum_gps"
    id = Column(Integer, primary_key=True, index=True)
    batch_id = Column(String, unique=True)
    fecha_proceso = Column(DateTime, default=datetime.utcnow)
    fecha_desde = Column(DateTime)
    fecha_hasta = Column(DateTime)
    semana_inicio = Column(Integer)  # Semana de inicio configurada (1 o 2)
    
    # Relationships
    frecuencias = relationship("FrecuenciaModel", back_populates="axum_gps", cascade="all, delete-orphan")
    frecuencia_headers = relationship("FrecuenciaHeaderModel", back_populates="axum_gps", cascade="all, delete-orphan")
    horas_vendedores = relationship("HorasHeaderModel", back_populates="axum_gps", cascade="all, delete-orphan")
    horas_detalle = relationship("HorasDetalleModel", back_populates="axum_gps", cascade="all, delete-orphan")

class FrecuenciaHeaderModel(Base):
    __tablename__ = "frecuencia_header"
    id = Column(Integer, primary_key=True, index=True)
    axum_gps_id = Column(Integer, ForeignKey("axum_gps.id"))
    vendedor = Column(String)
    tiempo_pdv_total = Column(String)  # Suma de tiempo_pdv_original
    
    # Relationships
    axum_gps = relationship("AxumGpsModel", back_populates="frecuencia_headers")

class FrecuenciaModel(Base):
    __tablename__ = "frecuencia"
    id = Column(Integer, primary_key=True, index=True)
    axum_gps_id = Column(Integer, ForeignKey("axum_gps.id"))
    batch_id = Column(String, index=True) 
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

    # Relationships
    axum_gps = relationship("AxumGpsModel", back_populates="frecuencias")

class HorasHeaderModel(Base):
    __tablename__ = "horas_header"
    id = Column(Integer, primary_key=True, index=True)
    axum_gps_id = Column(Integer, ForeignKey("axum_gps.id"))
    vendedor = Column(String)
    horas_totales = Column(String) # HH:MM:SS
    dias_trabajados = Column(Integer)
    promedio_horas_diarias = Column(String) # HH:MM:SS
    promedio_checkin = Column(String) # HH:MM:SS
    promedio_checkout = Column(String) # HH:MM:SS
    viatico = Column(Float, default=0.0)
    linea = Column(String)

    # Relationships
    axum_gps = relationship("AxumGpsModel", back_populates="horas_vendedores")

class HorasDetalleModel(Base):
    __tablename__ = "horas"
    id = Column(Integer, primary_key=True, index=True)
    axum_gps_id = Column(Integer, ForeignKey("axum_gps.id"))
    vendedor = Column(String)
    cliente = Column(String)  # Extraído del campo Codigo (ej: 23453.239 -> 23453)
    fecha = Column(DateTime)  # Día de trabajo
    primer_checkin = Column(DateTime)
    ultimo_checkout = Column(DateTime)
    total_horas_dia = Column(String)  # HH:MM:SS diferencia entre checkout y checkin
    
    # Relationships
    axum_gps = relationship("AxumGpsModel", back_populates="horas_detalle")

class ViaticoConfigModel(Base):
    __tablename__ = "viatico_config"
    id = Column(Integer, primary_key=True, index=True)
    zona = Column(String, unique=True) # CABA_GBA, INTERIOR
    valor = Column(Float)

import time

def init_db():
    retries = 5
    while retries > 0:
        try:
            Base.metadata.create_all(bind=engine)
            db = SessionLocal()
            if db.query(ViaticoConfigModel).count() == 0:
                db.add(ViaticoConfigModel(zona="CABA_GBA", valor=0.0))
                db.add(ViaticoConfigModel(zona="INTERIOR", valor=0.0))
                db.commit()
            db.close()
            print("Conexión a la base de datos exitosa.")
            break
        except Exception as e:
            retries -= 1
            print(f"Error conectando a la base de datos. ({retries} intentos restantes): {e}")
            time.sleep(5)
    if retries == 0:
        print("No se pudo conectar a la base de datos después de varios intentos.")
