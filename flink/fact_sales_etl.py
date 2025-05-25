# === Файл: batch_fact_sales.py ===

from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Batch Environment
settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(environment_settings=settings)

# 2. PostgreSQL Sources
print("📥 Registering dim tables as batch sources...")

jdbc_options = {
    'connector': 'jdbc',
    'url': 'jdbc:postgresql://postgres:5432/bigdata',
    'username': 'user',
    'password': 'pass',
    'driver': 'org.postgresql.Driver'
}

t_env.execute_sql(f"""
CREATE TABLE dim_customers (
    customer_id INT,
    email STRING
) WITH ({', '.join([f"'{k}' = '{v}'" for k, v in jdbc_options.items()])}, 'table-name' = 'dim_customers')
""")

t_env.execute_sql(f"""
CREATE TABLE dim_sellers (
    seller_id INT,
    email STRING
) WITH ({', '.join([f"'{k}' = '{v}'" for k, v in jdbc_options.items()])}, 'table-name' = 'dim_sellers')
""")

t_env.execute_sql(f"""
CREATE TABLE dim_products (
    product_id INT,
    name STRING,
    category STRING
) WITH ({', '.join([f"'{k}' = '{v}'" for k, v in jdbc_options.items()])}, 'table-name' = 'dim_products')
""")

t_env.execute_sql(f"""
CREATE TABLE dim_stores (
    store_id INT,
    email STRING
) WITH ({', '.join([f"'{k}' = '{v}'" for k, v in jdbc_options.items()])}, 'table-name' = 'dim_stores')
""")

t_env.execute_sql(f"""
CREATE TABLE dim_suppliers (
    supplier_id INT,
    email STRING
) WITH ({', '.join([f"'{k}' = '{v}'" for k, v in jdbc_options.items()])}, 'table-name' = 'dim_suppliers')
""")

# 3. Kafka Batch Source (чтение JSON сообщений как таблицу)
t_env.execute_sql("""
CREATE TABLE kafka_source (
    sale_date STRING,
    sale_quantity INT,
    sale_total_price DECIMAL(10,2),
    customer_email STRING,
    seller_email STRING,
    product_name STRING,
    product_category STRING,
    store_email STRING,
    supplier_email STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'mock-data',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'fact-batch-job',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
)
""")

# 4. Sink: fact_sales
print("🛠 Creating fact_sales sink for batch job...")
t_env.execute_sql(f"""
CREATE TABLE fact_sales_sink (
    sale_date DATE,
    quantity INT,
    total_price DECIMAL(10,2),
    customer_id INT,
    seller_id INT,
    product_id INT,
    store_id INT,
    supplier_id INT
) WITH ({', '.join([f"'{k}' = '{v}'" for k, v in jdbc_options.items()])}, 'table-name' = 'fact_sales')
""")

# 5. Вставка
print("📄 Inserting fact_sales via batch joins...")
t_env.execute_sql("""
INSERT INTO fact_sales_sink
SELECT
    TO_DATE(k.sale_date, 'MM/DD/YYYY'),
    k.sale_quantity,
    k.sale_total_price,
    c.customer_id,
    s.seller_id,
    p.product_id,
    st.store_id,
    sup.supplier_id
FROM kafka_source AS k
JOIN dim_customers AS c ON k.customer_email = c.email
JOIN dim_sellers AS s ON k.seller_email = s.email
JOIN dim_products AS p ON k.product_name = p.name AND k.product_category = p.category
JOIN dim_stores AS st ON k.store_email = st.email
JOIN dim_suppliers AS sup ON k.supplier_email = sup.email
""")

print("✅ fact_sales inserted in batch mode.")
