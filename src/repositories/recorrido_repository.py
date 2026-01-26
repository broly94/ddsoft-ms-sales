import pandas as pd
from src.models.route_models import SessionLocal, RecorridoModel

class RecorridoRepository:
    @staticmethod
    def save_all(df_prog: pd.DataFrame):
        db = SessionLocal()
        try:
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

    @staticmethod
    def get_all_as_df():
        db = SessionLocal()
        try:
            items = db.query(RecorridoModel).all()
            if not items:
                return pd.DataFrame()
            data = [{
                'Clave': item.clave,
                'Vendedor': item.vendedor,
                'Cliente': item.cliente,
                'Dia_Prog': item.dia_prog,
                'Semana_Prog': item.semana_prog,
                'Bloque': item.bloque,
                'Linea_Origen': item.linea_origen,
                'Texto_Original': item.texto_original
            } for item in items]
            return pd.DataFrame(data)
        finally:
            db.close()

    @staticmethod
    def get_filtered(limit=100, offset=0, **filters):
        db = SessionLocal()
        try:
            query = db.query(RecorridoModel)
            if filters.get('vendedor'):
                query = query.filter(RecorridoModel.vendedor.ilike(f"%{filters['vendedor']}%"))
            if filters.get('cliente'):
                query = query.filter(RecorridoModel.cliente.ilike(f"%{filters['cliente']}%"))
            if filters.get('linea') and filters['linea'] != "all":
                query = query.filter(RecorridoModel.linea_origen.ilike(f"%{filters['linea']}%"))
            if filters.get('bloque') and filters['bloque'] != "all":
                query = query.filter(RecorridoModel.bloque.ilike(f"%{filters['bloque']}%"))
            if filters.get('semana') and filters['semana'] != 0:
                query = query.filter(RecorridoModel.semana_prog == filters['semana'])
            if filters.get('dia') and filters['dia'] != "all":
                query = query.filter(RecorridoModel.dia_prog.ilike(f"{filters['dia']}"))
            
            total = query.count()
            items = query.order_by(RecorridoModel.id.desc()).offset(offset).limit(limit).all()
            return items, total
        finally:
            db.close()

    @staticmethod
    def get_by_id(recorrido_id: int):
        db = SessionLocal()
        try:
            return db.query(RecorridoModel).filter(RecorridoModel.id == recorrido_id).first()
        finally:
            db.close()

    @staticmethod
    def create(data: dict):
        db = SessionLocal()
        try:
            db_item = RecorridoModel(**data)
            db.add(db_item)
            db.commit()
            db.refresh(db_item)
            return db_item
        finally:
            db.close()

    @staticmethod
    def update(recorrido_id: int, data: dict):
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

    @staticmethod
    def delete(recorrido_id: int):
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
