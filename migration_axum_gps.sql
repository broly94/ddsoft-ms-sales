-- Migración completa: Reestructuración de tablas
-- Ejecutar en PostgreSQL

-- 1. Eliminar tablas existentes (CUIDADO: Esto borrará datos)
DROP TABLE IF EXISTS horas CASCADE;
DROP TABLE IF EXISTS horas_header CASCADE;
DROP TABLE IF EXISTS frecuencia CASCADE;
DROP TABLE IF EXISTS frecuencia_header CASCADE;

-- 2. Renombrar frecuencia_header → axum_gps
-- (Si ya existe, primero eliminarla con el DROP de arriba)

-- 3. Crear tabla axum_gps (metadata del procesamiento)
CREATE TABLE axum_gps (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR UNIQUE,
    fecha_proceso TIMESTAMP DEFAULT NOW(),
    fecha_desde TIMESTAMP,
    fecha_hasta TIMESTAMP
);

-- 4. Crear tabla frecuencia_header (agregación por vendedor)
CREATE TABLE frecuencia_header (
    id SERIAL PRIMARY KEY,
    axum_gps_id INTEGER REFERENCES axum_gps(id) ON DELETE CASCADE,
    vendedor VARCHAR,
    tiempo_pdv_total VARCHAR
);

-- 5. Crear tabla frecuencia (detalle de visitas)
CREATE TABLE frecuencia (
    id SERIAL PRIMARY KEY,
    axum_gps_id INTEGER REFERENCES axum_gps(id) ON DELETE CASCADE,
    batch_id VARCHAR,
    vendedor VARCHAR,
    cliente VARCHAR,
    fecha_checkin TIMESTAMP,
    fecha_checkout TIMESTAMP,
    tiempo_pdv_original VARCHAR,
    tiempo_pdv_limitado VARCHAR,
    dia_real VARCHAR,
    semana_real INTEGER,
    programacion VARCHAR,
    bloque VARCHAR,
    linea VARCHAR,
    estado VARCHAR,
    fecha_proceso TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_frecuencia_batch ON frecuencia(batch_id);

-- 6. Crear tabla horas_header (resumen de horas por vendedor)
CREATE TABLE horas_header (
    id SERIAL PRIMARY KEY,
    axum_gps_id INTEGER REFERENCES axum_gps(id) ON DELETE CASCADE,
    vendedor VARCHAR,
    horas_totales VARCHAR,
    dias_trabajados INTEGER,
    promedio_horas_diarias VARCHAR,
    promedio_checkin VARCHAR,
    promedio_checkout VARCHAR,
    viatico FLOAT DEFAULT 0.0,
    linea VARCHAR
);

-- 7. Crear tabla horas (detalle diario de horas)
CREATE TABLE horas (
    id SERIAL PRIMARY KEY,
    axum_gps_id INTEGER REFERENCES axum_gps(id) ON DELETE CASCADE,
    vendedor VARCHAR,
    cliente VARCHAR,
    fecha TIMESTAMP,
    primer_checkin TIMESTAMP,
    ultimo_checkout TIMESTAMP,
    total_horas_dia VARCHAR
);

-- Confirmación
SELECT 'Migración completada exitosamente!' AS status;
