import sqlite3
import pandas as pd


class DatabaseManager:
    def __init__(self, db_path='../outputs/financial_dw.db'):
        self.DB_PATH = db_path

    def create_connection(self):
        return sqlite3.connect(self.DB_PATH)

    def create_database(self):
        connection = self.create_connection()
        cursor = connection.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id TEXT PRIMARY KEY,
            username TEXT,
            name TEXT,
            birthdate DATE
        ) 
        """)



        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_accounts (
            account_id INTEGER PRIMARY KEY,
            account_limit REAL
        )    
        """)

        # Tabla puente para relación customer-accounts
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_customers_accounts(
            customer_id TEXT NOT NULL,
            account_id INTEGER NOT NULL,
            PRIMARY KEY (customer_id, account_id),
            FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
            FOREIGN KEY (account_id) REFERENCES fact_accounts(account_id)
        )
        """)

        # Tabla de productos (DIMENSIÓN)
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS dim_products (
                    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_name TEXT UNIQUE
                )
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_account_products (
            account_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            PRIMARY KEY (account_id, product_id),
            FOREIGN KEY (account_id) REFERENCES fact_accounts(account_id),
            FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
        )
        """)

        cursor.execute("""
                CREATE TABLE IF NOT EXISTS fact_transactions (
                    transaction_id TEXT PRIMARY KEY,
                    account_id INTEGER,
                    transaction_date DATE,
                    amount REAL,
                    transaction_code TEXT,
                    symbol TEXT,
                    price REAL,            
                    total REAL,             
                    FOREIGN KEY (account_id) REFERENCES fact_accounts(account_id)
                )
        """)

        cursor.execute("""
                CREATE TABLE IF NOT EXISTS dim_tier_benefits (
                    tier_id TEXT NOT NULL,
                    tier TEXT NOT NULL,
                    benefit TEXT NOT NULL,
                    active BOOLEAN NOT NULL,
                    customer_id TEXT NOT NULL,
                    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
                )
        """)

        connection.commit()
        connection.close()

