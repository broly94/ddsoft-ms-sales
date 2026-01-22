import pandas as pd
from datetime import datetime, timedelta
import re
import io

class RouteValidator:
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

    @staticmethod
    def calcular_semana_logica(fecha_objetivo):
        # Fecha ancla definida en el código original
        fecha_ancla = datetime(2025, 10, 27).date()
        if isinstance(fecha_objetivo, datetime):
            fecha_objetivo = fecha_objetivo.date()
        delta_dias = (fecha_objetivo - fecha_ancla).days
        semanas_transcurridas = delta_dias // 7
        return 1 if semanas_transcurridas % 2 == 0 else 2

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
    def procesar_datos(cls, recorrido_content, horario_content, fecha_inicio_str, fecha_fin_str):
        df_prog = cls.procesar_recorrido(recorrido_content)
        return cls.procesar_con_df_prog(df_prog, horario_content, fecha_inicio_str, fecha_fin_str)

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
            df_horario['Es Valido'] = df_horario['Es Valido'].astype(str).str.upper().str.strip()
            df_horario = df_horario[df_horario['Es Valido'] == 'SI'].copy()
            
            # Normalizar fechas
            df_horario['Fecha Checkin'] = pd.to_datetime(df_horario['Fecha Checkin'], dayfirst=True, errors='coerce')
            df_horario['Fecha Checkout'] = pd.to_datetime(df_horario['Fecha Checkout'], dayfirst=True, errors='coerce')
            
            df_horario = df_horario[(df_horario['Fecha Checkin'] >= f_ini) & (df_horario['Fecha Checkin'] <= f_fin)]
        except Exception as e:
            raise ValueError(f"Error procesando archivo Horario: {e}")

        # Límite de 59:59
        LIMITE_TIEMPO_PDV = timedelta(hours=0, minutes=59, seconds=59)
        df_horario['Tiempo_Delta'] = pd.to_timedelta(df_horario['Tiempo en PDV'].astype(str), errors='coerce')
        df_horario['Tiempo_PDV_Final'] = df_horario['Tiempo_Delta'].apply(
            lambda x: LIMITE_TIEMPO_PDV if x > LIMITE_TIEMPO_PDV else x
        )
        df_horario['Tiempo_PDV_String'] = df_horario['Tiempo_PDV_Final'].apply(
            lambda x: f"{int(x.total_seconds() // 3600):02}:{int((x.total_seconds() % 3600) // 60):02}:{int(x.total_seconds() % 60):02}"
        )

        resultados = []
        dias_esp = {0: 'Lunes', 1: 'Martes', 2: 'Miercoles', 3: 'Jueves', 4: 'Viernes', 5: 'Sabado', 6: 'Domingo'}
        def normalizar_dia(d): return d.replace('é','e').replace('á','a').replace('í','i').capitalize()

        # Determinar el ancla basado en el parámetro semana_inicio
        # Si queremos que f_ini sea semana X, ajustamos el cálculo
        # La lógica original era: 2025-10-27 es inicio de semana 1.
        # Calcularemos la diferencia de semanas desde f_ini.
        
        for _, visita in df_horario.iterrows():
            fecha = visita['Fecha Checkin']
            if pd.isna(fecha): continue
            if fecha.weekday() == 6: continue 
                
            codigo_raw = str(visita.get('Codigo', ''))
            vendedor_col = str(visita.get('Vendedor', '')).strip()
            
            parts = codigo_raw.split('.')
            cliente_raw = parts[0]
            cliente_clean = cls.limpiar_id(cliente_raw)
            
            if vendedor_col and vendedor_col.lower() != 'nan':
                vendedor_final = cls.limpiar_id(vendedor_col)
            elif len(parts) > 1:
                vendedor_final = cls.limpiar_id(parts[1])
            else:
                continue

            # Cálculo de semana relativa a la fecha de inicio y la semana de inicio proporcionada
            days_diff = (fecha.date() - f_ini.date()).days
            # Ajustar para que el lunes de esa semana sea el punto de referencia
            days_to_monday = fecha.weekday() # 0=Lunes
            monday_of_visita = fecha.date() - timedelta(days=days_to_monday)
            monday_of_start = f_ini.date() - timedelta(days=f_ini.weekday())
            
            weeks_diff = (monday_of_visita - monday_of_start).days // 7
            
            # Si semana_inicio es 1: 0->1, 1->2, 2->1, 3->2...
            # Si semana_inicio es 2: 0->2, 1->1, 2->2, 3->1...
            if semana_inicio == 1:
                semana_calculada = 1 if weeks_diff % 2 == 0 else 2
            else:
                semana_calculada = 2 if weeks_diff % 2 == 0 else 1

            dia_nombre_real = dias_esp[fecha.weekday()]
            dia_nombre_norm = normalizar_dia(dia_nombre_real)
            
            clave_busqueda = f"{cliente_clean}_{vendedor_final}_{dia_nombre_norm}_{semana_calculada}"
            coincidencia = dict_prog.get(clave_busqueda)
            
            if coincidencia is None:
                clave_alt = f"{cliente_clean}_{vendedor_final}_{dia_nombre_real}_{semana_calculada}"
                coincidencia = dict_prog.get(clave_alt)

            if coincidencia is not None:
                item = {
                    'Vendedor': vendedor_final,
                    'Cliente': cliente_clean,
                    'Fecha_Checkin': fecha,
                    'Fecha_Checkout': visita['Fecha Checkout'],
                    'Tiempo_PDV_Original': visita.get('Tiempo en PDV'),
                    'Tiempo_PDV_Limitado': visita['Tiempo_PDV_String'],
                    'Dia_Real': dia_nombre_real,
                    'Semana_Real': semana_calculada,
                    'Programacion': coincidencia['Texto_Original'],
                    'Bloque': coincidencia['Bloque'],
                    'Linea': coincidencia['Linea_Origen'],
                    'Estado': 'COINCIDE',
                    '_timedelta': visita['Tiempo_PDV_Final']
                }
                resultados.append(item)

        df_res = pd.DataFrame(resultados)
        if df_res.empty:
            return pd.DataFrame(), pd.DataFrame(), df_prog

        df_res = df_res.sort_values('Fecha_Checkin')
        
        # Resumen de horas
        df_horas_calc = df_res[['Vendedor', '_timedelta']].copy()
        df_resumen_horas = df_horas_calc.groupby('Vendedor')['_timedelta'].sum().reset_index()
        
        def format_total_time(td):
            if pd.isna(td): return "00:00:00"
            total_seconds = int(td.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            return f"{hours:02}:{minutes:02}:{seconds:02}"
        
        df_resumen_horas['Tiempo_de_Venta'] = df_resumen_horas['_timedelta'].apply(format_total_time)
        df_sheet_horas = df_resumen_horas[['Vendedor', 'Tiempo_de_Venta']]

        # Limpiar para exportar
        df_res_clean = df_res.drop(columns=['_timedelta'])
        
        return df_res_clean, df_sheet_horas, df_prog
