"""
Migración: Agregar columna semana_inicio a tabla axum_gps

Ejecutar con:
python migrate_add_semana_inicio.py
"""

from sqlalchemy import create_engine, text
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user_sales:password_sales@localhost:5433/db_sales")

def migrate():
    engine = create_engine(DATABASE_URL)
    
    with engine.connect() as conn:
        print("🔧 Iniciando migración: Agregar semana_inicio a axum_gps")
        
        # Verificar si la columna ya existe
        check_query = text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='axum_gps' AND column_name='semana_inicio';
        """)
        
        result = conn.execute(check_query)
        exists = result.fetchone()
        
        if exists:
            print("✅ La columna 'semana_inicio' ya existe en la tabla 'axum_gps'")
        else:
            # Agregar la columna
            alter_query = text("""
                ALTER TABLE axum_gps 
                ADD COLUMN semana_inicio INTEGER DEFAULT 1;
            """)
            
            conn.execute(alter_query)
            conn.commit()
            print("✅ Columna 'semana_inicio' agregada exitosamente a 'axum_gps'")
            print("   - Tipo: INTEGER")
            print("   - Default: 1")
        
        print("🎉 Migración completada!")

if __name__ == "__main__":
    migrate()
