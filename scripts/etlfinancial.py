from databasemanager import DatabaseManager
from dataextractor import DataExtractor
from datatransformer import DataTransformer
from dataloader import DataLoader

class ETLFinancial:
    def __init__(self, db_path='../outputs/financial_dw.db'):
        self.db_manager = DatabaseManager(db_path)
        self.data_loader = DataLoader(self.db_manager)
        self.data_sources = {
            'accounts': '../data/sample_analytics.accounts.json',
            'customers': '../data/sample_analytics.customers.json',
            'transactions': '../data/sample_analytics.transactions.json'
        }

    def run(self):
        print("Creando base de datos...")
        self.db_manager.create_database()

        print("Extrayendo datos...")
        accounts = DataExtractor.extract_data(self.data_sources['accounts'])
        customers = DataExtractor.extract_data(self.data_sources['customers'])
        transactions = DataExtractor.extract_data(self.data_sources['transactions'])

        print("Transformando datos...")
        dim_customers = DataTransformer.transform_customers(customers)
        dim_tier_benefits = DataTransformer.transform_tier_benefits(customers)
        products_data = DataTransformer.transform_products(accounts)
        dim_products = products_data['dim_products']
        fact_account_products = products_data['fact_account_products']
        fact_accounts = DataTransformer.transform_accounts(accounts)
        fact_customer_accounts = DataTransformer.transform_customer_accounts(customers)
        fact_transactions = DataTransformer.transform_transactions(transactions)

        print("Cargando datos...")
        # 1. Cargar tablas DIM primero
        self.data_loader.load_data('dim_customers', dim_customers)
        self.data_loader.load_data('dim_tier_benefits', dim_tier_benefits)
        self.data_loader.load_data('dim_products', dim_products)
        self.data_loader.load_data('dim_accounts', fact_accounts)

        # 2. Cargar tablas de hechos
        self.data_loader.load_data('fact_customers_accounts', fact_customer_accounts)

        # 3. Usar metodo especifico para account_products
        self.data_loader.load_account_products(fact_account_products)

        self.data_loader.load_data('fact_transactions', fact_transactions)

        print("ETL completado exitosamente!")


if __name__ == "__main__":
    etl = ETLFinancial()
    etl.run()