import hashlib
from datetime import datetime

class DataTransformer:

    @staticmethod
    def generate_customer_id(customer_data):
        if not customer_data:
            return None

        # Usar siempre el ObjectId de MongoDB cuando esté disponible
        if '_id' in customer_data and '$oid' in customer_data['_id']:
            raw_id = customer_data['_id']['$oid']
        else:
            # Combinar más campos para garantizar unicidad
            raw_id = f"{customer_data.get('username', '')}-{customer_data.get('name', '')}-{customer_data.get('address', '')}"

        # Usar hash más largo para reducir colisiones
        hash_object = hashlib.sha256(raw_id.encode())
        return hash_object.hexdigest()


    @staticmethod
    def transform_customers(customers_data):
        transformed = []
        seen_ids = set()
        for customer in customers_data:
            # Procesar birthdate (sin cambios)
            birthdate_data = customer.get('birthdate', {})
            birthdate = None
            if isinstance(birthdate_data, dict) and '$date' in birthdate_data:
                iso_date = birthdate_data['$date']
                if len(iso_date) >= 10:
                    birthdate = iso_date[:10]

            # Generar customer_id a partir del username
            username = customer.get('username')
            customer_id = DataTransformer.generate_customer_id(customer)
            # Verificar duplicados antes de agregar
            if customer_id in seen_ids:
                print(f"Advertencia: ID duplicado detectado - {customer_id}")
                continue
            seen_ids.add(customer_id)

            transformed.append({
                'customer_id': str(customer_id),
                'username': username,
                'name': customer.get('name'),
                'birthdate': birthdate
            })
        return transformed

    @staticmethod
    def transform_tier_benefits(customers_data):
        transformed = []
        for customer in customers_data:
            customer_id = DataTransformer.generate_customer_id(customer)

            tier_details = customer.get('tier_and_details', {})
            for tier_id, tier_info in tier_details.items():
                # Crear una entrada por cada beneficio
                for benefit in tier_info.get('benefits', []):
                    transformed.append({
                        'tier_id': tier_id,
                        'tier': tier_info.get('tier', ''),
                        'benefit': benefit,
                        'active': 1 if tier_info.get('active', False) else 0,
                        'customer_id': customer_id
                    })
        return transformed

    @staticmethod
    def transform_products(accounts_data):
        unique_products = set()
        account_products = []

        for account in accounts_data:
            account_id = account['account_id']
            seen_products = set()  # Evitar duplicados por cuenta

            for product in account.get('products', []):
                if product not in seen_products:
                    seen_products.add(product)
                    unique_products.add(product)
                    account_products.append({
                        'account_id': account_id,
                        'product_name': product
                    })

        return {
            'dim_products': [{'product_name': p} for p in unique_products],
            'fact_account_products': account_products
        }

    @staticmethod
    def transform_accounts(accounts_data):
        transformed = []
        for account in accounts_data:
            transformed.append({
                'account_id': account.get('account_id'),
                'account_limit': account.get('limit', 0)
            })
        return transformed

    @staticmethod
    def transform_customer_accounts(customer_data):
        transformed = []
        for customer in customer_data:
            customer_id = DataTransformer.generate_customer_id(customer)
            for account in customer.get('accounts', []):
                transformed.append({
                    'customer_id': customer_id,
                    'account_id' : int(account)
                })
        return transformed

    @staticmethod
    def generate_transaction_id(account_id, transaction_data):
        """Genera un ID hexadecimal para la transacción usando hash SHA-256"""
        raw_string = f"{account_id}-{transaction_data['date']['$date']}-{transaction_data['amount']}-{transaction_data.get('symbol', '')}"
        hash_bytes = hashlib.sha256(raw_string.encode())
        return hash_bytes.hexdigest()[:16]

    @staticmethod
    def transform_transactions(transactions_bucket_data):
        transformed = []
        for bucket in transactions_bucket_data:
            account_id = bucket['account_id']

            for transaction in bucket.get('transactions', []):
                # Generar transaction_id único
                transaction_id = DataTransformer.generate_transaction_id(account_id, transaction)

                # Procesar fecha de transacción
                date_info = transaction.get('date', {})
                transaction_date = None

                if isinstance(date_info, dict) and '$date' in date_info:
                    iso_date = date_info['$date']
                    if len(iso_date) >= 10:
                        transaction_date = iso_date[:10]



                transformed.append({
                    'transaction_id': str(transaction_id),
                    'account_id': account_id,
                    'transaction_date': transaction_date,
                    'amount': transaction.get('amount', 0),
                    'transaction_code': transaction.get('transaction_code'),
                    'symbol': transaction.get('symbol'),
                    'price': transaction.get('price'),
                    'total': transaction.get('total')
                })
        return transformed