-- 1. Estadísticas de límites de cuentas
SELECT
    ROUND(AVG(account_limit), 2) AS promedio,
    MIN(account_limit) AS minimo,
    MAX(account_limit) AS maximo,
    ROUND(
        SQRT(
            (SUM(account_limit * account_limit) * 1.0 / COUNT(*))
            - POWER(AVG(account_limit), 2)
        ),
        2 ) AS desviacion_estandar
FROM dim_accounts;

-- 2. Clientes con múltiples cuentas
SELECT COUNT(customer_id) AS clientes_multiples
FROM (
    SELECT customer_id
    FROM fact_customers_accounts
    GROUP BY customer_id
    HAVING COUNT(account_id) > 1
);

-- 3. Transacciones de junio
SELECT
    ROUND(AVG(amount), 2) AS monto_promedio,
    COUNT(*) AS total_transacciones
FROM fact_transactions
WHERE strftime('%m', transaction_date) = '06';

-- 4. Cuenta con mayor diferencia en transacciones
SELECT
    account_id,
    MAX(amount) - MIN(amount) AS diferencia
FROM fact_transactions
GROUP BY account_id
ORDER BY diferencia DESC
LIMIT 1;

-- 5. Cuentas con 3 productos incluyendo 'Commodity'
SELECT COUNT(*) AS cuentas_con_3_productos_y_commodity
FROM (
    SELECT account_id
    FROM fact_account_products fap
    JOIN dim_products dp ON fap.product_id = dp.product_id
    GROUP BY account_id
    HAVING
        COUNT(*) = 3  -- Exactamente 3 productos
        AND SUM(CASE WHEN dp.product_name = 'Commodity' THEN 1 ELSE 0 END) >= 1  -- Incluye Commodity
);

-- 6. Cliente con más transacciones tipo sell
SELECT c.name AS nombre_cliente
FROM dim_customers c
WHERE c.customer_id = (
    SELECT fca.customer_id
    FROM fact_transactions t
    JOIN fact_customers_accounts fca ON t.account_id = fca.account_id
    WHERE t.transaction_code = 'sell'
    GROUP BY fca.customer_id
    ORDER BY COUNT(*) DESC
    LIMIT 1
);

-- 7. Cliente con 10/20 operaciones 'buy' y presenta promedio de inversion mas alto
SELECT c.username
FROM (
    SELECT
        t.account_id,
        COUNT(*) AS total_buys,
        AVG(t.total) AS avg_inversion
    FROM fact_transactions t
    WHERE t.transaction_code = 'buy'
    GROUP BY t.account_id
    HAVING COUNT(*) BETWEEN 10 AND 20
) AS bt
JOIN fact_customers_accounts fca ON bt.account_id = fca.account_id
JOIN dim_customers c ON fca.customer_id = c.customer_id
ORDER BY bt.avg_inversion DESC
LIMIT 1;

-- 8. Promedio de transacciones por acción
SELECT
    symbol,
    COUNT(CASE WHEN transaction_code = 'buy' THEN 1 END) AS total_buys,
    COUNT(CASE WHEN transaction_code = 'sell' THEN 1 END) AS total_sells
FROM fact_transactions
WHERE transaction_code IN ('buy', 'sell')
GROUP BY symbol;

-- 9. Beneficios de clientes Gold
SELECT DISTINCT name AS name, benefit AS benefit
FROM dim_tier_benefits dtb
JOIN dim_customers dc ON dc.customer_id = dtb.customer_id
WHERE tier = 'Gold';

-- 10. Clientes por rango etario con compras de AMZN
SELECT
    CASE
        WHEN birthdate IS NULL THEN 'SIN FECHA'
        ELSE '[' || CAST(FLOOR(edad / 10) * 10 AS TEXT) || '-' || CAST(FLOOR(edad / 10) * 10 + 9 AS TEXT) || ']'
    END AS rango_etario,
    COUNT(DISTINCT customer_id) AS cantidad_clientes
FROM (
    SELECT
        c.customer_id,
        c.birthdate,
        CAST((julianday('2025-05-16') - julianday(c.birthdate)) / 365.25 AS INTEGER) AS edad
    FROM dim_customers c
    JOIN fact_customers_accounts fca ON c.customer_id = fca.customer_id
    JOIN fact_transactions t ON fca.account_id = t.account_id
    WHERE t.transaction_code = 'buy'
      AND t.symbol = 'amzn'
) AS sub
GROUP BY rango_etario
ORDER BY rango_etario;