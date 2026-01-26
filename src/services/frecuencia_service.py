from src.repositories.frecuencia_repository import FrecuenciaRepository

class FrecuenciaService:
    @staticmethod
    def process_and_save_batch(visitas: list, horas_resumen: list, horas_detalle: list, metadata: dict):
        # 1. Fetch viatico configs
        viaticos = FrecuenciaRepository.get_viatico_configs()
        v_map = {v.zona: v.valor for v in viaticos}
        
        # 2. Enrich hours summary with actual viatico value based on Linea
        for h in horas_resumen:
            if h.get('Aplica_Viatico'):
                linea = str(h.get('Linea', '')).upper()
                if 'INTERIOR' in linea:
                    h['Viatico'] = v_map.get('INTERIOR', 0.0)
                else:
                    h['Viatico'] = v_map.get('CABA_GBA', 0.0)
            else:
                h['Viatico'] = 0.0
        
        # 3. Save to DB via repository (both summary and detail)
        batch_id = FrecuenciaRepository.save_batch(visitas, horas_resumen, horas_detalle, metadata)
        return batch_id

    @staticmethod
    def get_history():
        return FrecuenciaRepository.get_history()

    @staticmethod
    def get_viatico_configs():
        return FrecuenciaRepository.get_viatico_configs()

    @staticmethod
    def update_viatico_config(zona, valor):
        return FrecuenciaRepository.update_viatico_config(zona, valor)
