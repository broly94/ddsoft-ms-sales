from src.repositories.frecuencia_repository import FrecuenciaRepository

class FrecuenciaService:
    @staticmethod
    def process_and_save_batch(visitas: list, horas_resumen: list, horas_detalle: list, metadata: dict):
        viaticos = FrecuenciaRepository.get_viatico_configs()
        v_map = {v.zona: v.valor for v in viaticos}
        
        for h in horas_resumen:
            if h.get('Aplica_Viatico'):
                linea = str(h.get('Linea', '')).upper()
                if 'INTERIOR' in linea:
                    h['Viatico'] = v_map.get('INTERIOR', 0.0)
                else:
                    h['Viatico'] = v_map.get('CABA_GBA', 0.0)
            else:
                h['Viatico'] = 0.0
        
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
    
    @staticmethod
    def get_batch_details(batch_id: str):
        return FrecuenciaRepository.get_batch_details_with_cliente_count(batch_id)
    
    @staticmethod
    def get_batch_hours(batch_id: str):
        """Obtiene el resumen de horas de un batch específico"""
        return FrecuenciaRepository.get_batch_hours(batch_id)
    
    @staticmethod
    def get_recent_frecuencia(limit: int = 50, vendedor: str = None, cliente: str = None, batch_id: str = None):
        """Obtiene las últimas N visitas de frecuencia con filtros"""
        return FrecuenciaRepository.get_recent_frecuencia(limit, vendedor, cliente, batch_id)
    
    @staticmethod
    def get_frecuencia_summary(vendedor: str = None, batch_id: str = None):
        """Obtiene el resumen de tiempos por vendedor"""
        return FrecuenciaRepository.get_frecuencia_summary(vendedor, batch_id)
