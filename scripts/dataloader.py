import pandas as pd

class DataLoader:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def load_data(self, table_name, data):
        if not data:
            return

        conn = self.db_manager.create_connection()
        cursor = conn.cursor()

        # Obtener columnas de la tabla
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall()]

        # Construir query con manejo de duplicados
        placeholders = ', '.join(['?'] * len(columns))
        query = f"""
            INSERT OR IGNORE INTO {table_name} 
            VALUES ({placeholders})
        """

        # Preparar datos
        records = []
        for item in data:
            record = tuple(item.get(col) for col in columns)
            records.append(record)

        # Insertar evitando duplicados
        cursor.executemany(query, records)
        conn.commit()
        conn.close()

    # dataloader.py
    def load_account_products(self, account_products_data):
        if not account_products_data:
            return

        conn = self.db_manager.create_connection()
        cursor = conn.cursor()

        # Obtener mapeo de productos
        cursor.execute("SELECT product_id, product_name FROM dim_products")
        product_map = {row[1]: row[0] for row in cursor.fetchall()}

        # Preparar datos únicos usando un conjunto
        unique_records = set()
        for item in account_products_data:
            product_id = product_map.get(item['product_name'])
            if product_id:
                # Usar tupla para garantizar unicidad
                unique_records.add((item['account_id'], product_id))

        # Insertar evitando duplicados
        if unique_records:
            cursor.executemany(
                """INSERT OR IGNORE INTO fact_account_products 
                   (account_id, product_id) VALUES (?, ?)""",
                list(unique_records)
            )
            conn.commit()

        conn.close()