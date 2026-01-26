import pandas as pd
from src.repositories.recorrido_repository import RecorridoRepository
from src.services.validator_service import ValidatorService

class RecorridoService:
    @staticmethod
    def upload_recorrido(file_content):
        df_prog = ValidatorService.procesar_recorrido(file_content)
        if not df_prog.empty:
            RecorridoRepository.save_all(df_prog)
        return df_prog

    @staticmethod
    def get_all_recorridos(limit=100, offset=0, **filters):
        return RecorridoRepository.get_filtered(limit=limit, offset=offset, **filters)

    @staticmethod
    def get_recorrido_by_id(recorrido_id: int):
        return RecorridoRepository.get_by_id(recorrido_id)

    @staticmethod
    def create_recorrido(data: dict):
        return RecorridoRepository.create(data)

    @staticmethod
    def update_recorrido(recorrido_id: int, data: dict):
        return RecorridoRepository.update(recorrido_id, data)

    @staticmethod
    def delete_recorrido(recorrido_id: int):
        return RecorridoRepository.delete(recorrido_id)
    
    @staticmethod
    def get_recorrido_from_db():
        return RecorridoRepository.get_all_as_df()
