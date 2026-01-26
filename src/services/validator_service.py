import pandas as pd
from datetime import datetime, timedelta
import re
import io

class ValidatorService:
    @staticmethod
    def limpiar_id(valor):
        try:
            val_str = str(valor).strip()
            if val_str.lower() == 'nan': return ''
            if '.' in val_str:
                return val_str.split('.')[0]
            return val_str
        except:
            return str(valor).strip()

    @classmethod
    def procesar_recorrido(cls, file_content):
        try:
            df = pd.read_excel(io.BytesIO(file_content))
            programacion = []
            col_vendedores = 'Vendedores'
            col_cliente = 'Cliente'
            
            for _, row in df.iterrows():
                vendedores_raw = str(row[col_vendedores])
                if pd.isna(row[col_vendedores]) or vendedores_raw.lower() == 'nan': continue
                lista_vendedores = [v.strip() for v in vendedores_raw.split('-') if v.strip()]
                cliente = cls.limpiar_id(row[col_cliente])
                bloque = row.get('Bloque', 'Sin Bloque')
                
                for idx, vend in enumerate(lista_vendedores):
                    numero_linea = idx + 1
                    if numero_linea > 3: break 
                    nombre_col = f'Linea {numero_linea}'
                    if nombre_col in row and pd.notna(row[nombre_col]):
                        contenido = str(row[nombre_col]).strip()
                        if contenido.lower() == 'nan' or not contenido: continue
                        dias_asignados = [d.strip() for d in contenido.split(',') if d.strip()]
                        for dia_str in dias_asignados:
                            match = re.match(r'([a-zA-ZáéíóúÁÉÍÓÚñÑ]+)\s+(\d+)', dia_str)
                            if match:
                                dia_nombre = match.group(1) 
                                sem_numero = int(match.group(2))
                                vend_limpio = cls.limpiar_id(vend)
                                programacion.append({
                                    'Clave': f"{cliente}_{vend_limpio}_{dia_nombre}_{sem_numero}", 
                                    'Vendedor': vend_limpio,
                                    'Cliente': cliente,
                                    'Dia_Prog': dia_nombre,
                                    'Semana_Prog': sem_numero,
                                    'Bloque': bloque,
                                    'Linea_Origen': nombre_col,
                                    'Texto_Original': dia_str
                                })
            return pd.DataFrame(programacion)
        except Exception as e:
            raise ValueError(f"Error procesando archivo Recorrido: {e}")

    @classmethod
    def procesar_con_df_prog(cls, df_prog, horario_content, fecha_inicio_str, fecha_fin_str, semana_inicio: int = 1):
        try:
            f_ini = datetime.strptime(fecha_inicio_str, "%d/%m/%Y")
            f_fin = datetime.strptime(fecha_fin_str, "%d/%m/%Y")
            f_fin = f_fin.replace(hour=23, minute=59, second=59)
        except ValueError:
            raise ValueError("Formato de fecha inválido. Use DD/MM/AAAA")

        if df_prog is None or df_prog.empty:
            raise ValueError("No se encontraron datos válidos en el Recorrido.")
        dict_prog = {row['Clave']: row for _, row in df_prog.iterrows()}

        try:
            df_horario = pd.read_excel(io.BytesIO(horario_content))
            # Normalizamos 'Es Valido' pero NO filtramos el dataframe original aquí
            # para que el cálculo de 'Horas' incluya tanto registros 'SI' como 'NO'
            df_horario['Es Valido_Norm'] = df_horario['Es Valido'].astype(str).str.upper().str.strip()
            
            df_horario['Fecha Checkin'] = pd.to_datetime(df_horario['Fecha Checkin'], dayfirst=True, errors='coerce')
            df_horario['Fecha Checkout'] = pd.to_datetime(df_horario['Fecha Checkout'], dayfirst=True, errors='coerce')
            df_horario = df_horario[(df_horario['Fecha Checkin'] >= f_ini) & (df_horario['Fecha Checkin'] <= f_fin)]
        except Exception as e:
            raise ValueError(f"Error procesando archivo Horario: {e}")

        LIMITE_TIEMPO_PDV = timedelta(hours=0, minutes=59, seconds=59)
        # Aseguramos que 'Tiempo en PDV' sea tratado como string para la conversión a timedelta
        df_horario['Tiempo_Delta'] = pd.to_timedelta(df_horario['Tiempo en PDV'].astype(str), errors='coerce')
        df_horario['Tiempo_PDV_Final'] = df_horario['Tiempo_Delta'].apply(
            lambda x: LIMITE_TIEMPO_PDV if pd.notna(x) and x > LIMITE_TIEMPO_PDV else x
        )
        df_horario['Tiempo_PDV_String'] = df_horario['Tiempo_PDV_Final'].apply(
            lambda x: f"{int(x.total_seconds() // 3600):02}:{int((x.total_seconds() % 3600) // 60):02}:{int(x.total_seconds() % 60):02}" if pd.notna(x) else "00:00:00"
        )

        resultados = []
        dias_esp = {0: 'Lunes', 1: 'Martes', 2: 'Miercoles', 3: 'Jueves', 4: 'Viernes', 5: 'Sabado', 6: 'Domingo'}
        def normalizar_dia(d): return d.replace('é','e').replace('á','a').replace('í','i').capitalize()

        # 1. Preparar datos base
        df_horario = df_horario.copy()
        df_horario['Solo_Fecha'] = df_horario['Fecha Checkin'].dt.date
        df_horario['weekday'] = df_horario['Fecha Checkin'].dt.weekday
        
        # Mapa de vendedor -> linea desde la programación
        vendedor_linea_map = df_prog.set_index('Vendedor')['Linea_Origen'].to_dict()

        # Extraer Cliente y Vendedor de la columna 'Codigo' (formato cliente.vendedor)
        def extraer_ids(row):
            # Prioridad 1: Columna 'Vendedor'
            v_val = row.get('Vendedor')
            v_final = None
            if pd.notna(v_val) and str(v_val).strip() != '' and str(v_val).lower() != 'nan':
                v_final = cls.limpiar_id(v_val)
            
            # Prioridad 2: Columna 'Codigo' (formato cliente.vendedor)
            cod_val = row.get('Codigo')
            c_final = None
            if pd.notna(cod_val):
                parts = str(cod_val).split('.')
                c_final = cls.limpiar_id(parts[0])
                if not v_final and len(parts) > 1:
                    v_final = cls.limpiar_id(parts[1])
            
            return pd.Series([c_final, v_final], index=['Cliente_Clean', 'Vendedor_Final'])

        df_horario[['Cliente_Clean', 'Vendedor_Final']] = df_horario.apply(extraer_ids, axis=1)
        
        # FILTRO: Solo procesar registros con "Es Valido" == "SI" para el cálculo de HORAS
        df_horario = df_horario[df_horario['Es Valido_Norm'] == 'SI']
        
        # Solo es obligatorio tener el Vendedor. El cliente puede fallar.
        df_horario = df_horario.dropna(subset=['Vendedor_Final'])

        # 2. Cálculo de Horas - Generar RESUMEN y DETALLE
        resumen_data = []
        detalle_data = []
        
        for vendedor, group in df_horario.groupby('Vendedor_Final'):
            # Estadísticas diarias: Primer Checkin y Último Checkout del día
            daily_stats = group.groupby('Solo_Fecha').agg({
                'Fecha Checkin': 'min',
                'Fecha Checkout': 'max',
                'Cliente_Clean': 'first'  # Tomamos el primer cliente del día
            })
            
            # Cálculo de Jornada Diaria: Diferencia entre la última salida y la primera entrada
            daily_stats['Jornada_Laboral'] = daily_stats['Fecha Checkout'] - daily_stats['Fecha Checkin']
            
            # GENERAR DETALLE DIARIO
            for fecha, row in daily_stats.iterrows():
                if pd.notna(row['Jornada_Laboral']) and row['Jornada_Laboral'].total_seconds() > 0:
                    detalle_data.append({
                        'vendedor': vendedor,
                        'cliente': row['Cliente_Clean'] if pd.notna(row['Cliente_Clean']) else '',
                        'fecha': fecha.strftime('%Y-%m-%d') if hasattr(fecha, 'strftime') else str(fecha),
                        'primer_checkin': row['Fecha Checkin'].strftime('%Y-%m-%d %H:%M:%S'),
                        'ultimo_checkout': row['Fecha Checkout'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(row['Fecha Checkout']) else '',
                        'total_horas_dia': cls.format_total_time(row['Jornada_Laboral'])
                    })
            
            # Solo sumamos jornadas válidas y positivas para el RESUMEN
            positive_days = daily_stats[
                daily_stats['Jornada_Laboral'].notna() & 
                (daily_stats['Jornada_Laboral'].dt.total_seconds() > 0)
            ]['Jornada_Laboral']
            
            horas_totales_td = positive_days.sum()
            cant_dias = len(daily_stats) # Contamos todos los días con actividad
            prom_horas_td = horas_totales_td / cant_dias if cant_dias > 0 else timedelta(0)
            
            # Función auxiliar para convertir hora a segundos desde medianoche
            def to_sec(ts): return ts.hour * 3600 + ts.minute * 60 + ts.second
            
            # Promedios de horarios (IGNORANDO días donde falte el dato para no desvirtuar la media)
            avg_checkin_seg = daily_stats['Fecha Checkin'].dropna().apply(to_sec).mean()
            avg_checkout_seg = daily_stats['Fecha Checkout'].dropna().apply(to_sec).mean()
            
            # Si no hay datos válidos, ponemos 0
            avg_checkin_seg = avg_checkin_seg if pd.notna(avg_checkin_seg) else 0
            avg_checkout_seg = avg_checkout_seg if pd.notna(avg_checkout_seg) else 0
            
            # Lógica de Viático basada en promedios de extremos
            aplica_viatico = avg_checkin_seg < (9 * 3600) and avg_checkout_seg > (13 * 3600)
            
            def fmt_s(s):
                s = int(s)
                return f"{s//3600:02}:{(s%3600)//60:02}:{s%60:02}"

            resumen_data.append({
                'Vendedor': vendedor,
                'Horas_Totales': cls.format_total_time(horas_totales_td),
                'Dias_Trabajados': cant_dias,
                'Promedio_Horas': cls.format_total_time(prom_horas_td),
                'Promedio_Checkin': fmt_s(avg_checkin_seg),
                'Promedio_Checkout': fmt_s(avg_checkout_seg),
                'Aplica_Viatico': aplica_viatico,
                'Linea': vendedor_linea_map.get(vendedor, 'Sin Línea')
            })
        
        df_sheet_horas = pd.DataFrame(resumen_data)
        df_detalle_horas = pd.DataFrame(detalle_data)

        # 3. Lógica de coincidencia con programación (Frecuencia)
        df_validos = df_horario[df_horario['Es Valido_Norm'] == 'SI'].copy()
        monday_of_start = f_ini.date() - timedelta(days=f_ini.weekday())
        
        def calc_semana(fecha):
            monday_of_visita = fecha.date() - timedelta(days=fecha.weekday())
            weeks_diff = (monday_of_visita - monday_of_start).days // 7
            if semana_inicio == 1:
                return 1 if weeks_diff % 2 == 0 else 2
            return 2 if weeks_diff % 2 == 0 else 1

        df_validos['Semana_Calc'] = df_validos['Fecha Checkin'].apply(calc_semana)
        df_validos['Dia_Norm'] = df_validos['weekday'].map(dias_esp).apply(normalizar_dia)
        
        df_validos['Clave_Busqueda'] = (
            df_validos['Cliente_Clean'].astype(str) + "_" + 
            df_validos['Vendedor_Final'].astype(str) + "_" + 
            df_validos['Dia_Norm'] + "_" + 
            df_validos['Semana_Calc'].astype(str)
        )

        df_res_merged = df_validos.merge(df_prog, left_on='Clave_Busqueda', right_on='Clave', how='inner')

        resultados_final = []
        for _, row in df_res_merged.iterrows():
            resultados_final.append({
                'Vendedor': row['Vendedor_Final'],
                'Cliente': row['Cliente_Clean'],
                'Fecha_Checkin': row['Fecha Checkin'],
                'Fecha_Checkout': row['Fecha Checkout'],
                'Tiempo_PDV_Original': row.get('Tiempo en PDV'),
                'Tiempo_PDV_Limitado': row['Tiempo_PDV_String'],
                'Dia_Real': dias_esp[row['weekday']],
                'Semana_Real': row['Semana_Calc'],
                'Programacion': row['Texto_Original'],
                'Bloque': row['Bloque'],
                'Linea': row['Linea_Origen'],
                'Estado': 'COINCIDE'
            })
            
        df_res = pd.DataFrame(resultados_final)
        
        return df_res, df_sheet_horas, df_detalle_horas, df_prog

    @staticmethod
    def format_total_time(td):
        if pd.isna(td): return "00:00:00"
        total_seconds = int(td.total_seconds())
        return f"{total_seconds//3600:02}:{(total_seconds%3600)//60:02}:{total_seconds%60:02}"
