"""
Script de migración para reestructurar las tablas de horas.
Ejecutar una sola vez para actualizar la estructura de la base de datos.
"""

from src.models.route_models import Base, engine, SessionLocal
from sqlalchemy import text

def migrate_horas_tables():
    print("🔄 Iniciando migración de tablas de horas...")
    
    db = SessionLocal()
    try:
        # 1. Eliminar tabla 'horas' antigua (era resumen, ahora será detalle)
        print("  📦 Eliminando tabla 'horas' antigua...")
        db.execute(text("DROP TABLE IF EXISTS horas CASCADE"))
        db.commit()
        
        # 2. Crear tabla 'horas_header' (nueva tabla resumen)
        print("  📦 Creando tabla 'horas_header' (resumen)...")
        # Esta se crea automáticamente por SQLAlchemy
        
        # 3. Crear tabla 'horas' (nueva estructura detalle)
        print("  📦 Creando tabla 'horas' (detalle diario)...")
        # Esta también se crea automáticamente
        
        # 4. Recrear todas las tablas con la nueva estructura
        print("  🏗️  Recreando estructura completa...")
        Base.metadata.create_all(bind=engine)
        
        print("✅ Migración completada exitosamente!")
        print("\nNuevas tablas:")
        print("  - horas_header: Resumen por vendedor (totales, promedios)")
        print("  - horas: Detalle diario (vendedor, cliente, fecha, checkin, checkout)")
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error durante la migración: {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    print("="*60)
    print("MIGRACIÓN DE TABLAS DE HORAS")
    print("="*60)
    print("\n⚠️  ADVERTENCIA: Esta operación eliminará la tabla 'horas' existente.")
    print("    Los datos históricos en esa tabla se perderán.")
    print("\n¿Continuar? (escriba 'SI' para confirmar): ", end="")
    
    confirmacion = input().strip().upper()
    
    if confirmacion == "SI":
        migrate_horas_tables()
    else:
        print("\n❌ Migración cancelada por el usuario.")
