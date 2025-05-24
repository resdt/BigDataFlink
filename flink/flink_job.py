from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# 1. Set up environment
env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.in_streaming_mode()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# Optional: Drop records with NULL in NOT NULL columns
t_env.get_config().get_configuration().set_string(
    "table.exec.sink.not-null-enforcer", "drop"
)

print("🚀 Flink ETL job started")

# 2. Kafka source table
print("📦 Creating Kafka source...")
t_env.execute_sql(
    """
    CREATE TABLE kafka_source (
        id INT,
        proctime AS PROCTIME(),

        customer_first_name STRING,
        customer_last_name STRING,
        customer_age INT,
        customer_email STRING,
        customer_country STRING,
        customer_postal_code STRING,
        customer_pet_type STRING,
        customer_pet_name STRING,
        customer_pet_breed STRING,

        seller_first_name STRING,
        seller_last_name STRING,
        seller_email STRING,
        seller_country STRING,
        seller_postal_code STRING,

        product_name STRING,
        product_category STRING,
        product_price DECIMAL(10,2),
        product_quantity INT,
        
        sale_date STRING,
        sale_customer_id INT,
        sale_seller_id INT,
        sale_product_id INT,
        sale_quantity INT,
        sale_total_price DECIMAL(10,2),

        store_name STRING,
        store_location STRING,
        store_city STRING,
        store_state STRING,
        store_country STRING,
        store_phone STRING,
        store_email STRING,

        pet_category STRING,
        product_weight DECIMAL(10,2),
        product_color STRING,
        product_size STRING,
        product_brand STRING,
        product_material STRING,
        product_description STRING,
        product_rating DECIMAL(2,1),
        product_reviews INT,
        product_release_date STRING,
        product_expiry_date STRING,

        supplier_name STRING,
        supplier_contact STRING,
        supplier_email STRING,
        supplier_phone STRING,
        supplier_address STRING,
        supplier_city STRING,
        supplier_country STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'mock-data',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'flink-test-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """
)

# dim_customers_sink
print("🛠 Creating dim_customers_sink table...")
t_env.execute_sql(
    """
    CREATE TABLE dim_customers_sink (
        old_id INT,
        first_name STRING,
        last_name STRING,
        age INT,
        email STRING,
        country STRING,
        postal_code STRING,
        pet_type STRING,
        pet_name STRING,
        pet_breed STRING,
        PRIMARY KEY (old_id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/bigdata',
        'table-name' = 'dim_customers',
        'username' = 'user',
        'password' = 'pass',
        'driver' = 'org.postgresql.Driver'
    )
    """
)
print("📤 Inserting data into dim_customers_sink...")
t_env.execute_sql(
    """
    INSERT INTO dim_customers_sink
    SELECT
        id,
        customer_first_name,
        customer_last_name,
        customer_age,
        customer_email,
        customer_country,
        CAST(customer_postal_code AS STRING),
        customer_pet_type,
        customer_pet_name,
        customer_pet_breed
    FROM kafka_source
    """
)

# dim_sellers_sink
print("🛠 Creating dim_sellers_sink table...")
t_env.execute_sql(
    """
    CREATE TABLE dim_sellers_sink (
        old_id INT,
        first_name STRING,
        last_name STRING,
        email STRING,
        country STRING,
        postal_code STRING,
        PRIMARY KEY (old_id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/bigdata',
        'table-name' = 'dim_sellers',
        'username' = 'user',
        'password' = 'pass',
        'driver' = 'org.postgresql.Driver'
    )
    """
)
print("📄 Inserting data into dim_sellers_sink...")
t_env.execute_sql(
    """
    INSERT INTO dim_sellers_sink
    SELECT
        id,
        seller_first_name,
        seller_last_name,
        seller_email,
        seller_country,
        CAST(seller_postal_code AS STRING)
    FROM kafka_source
    """
)

# dim_products_sink
print("🛠 Creating dim_products_sink table...")
t_env.execute_sql(
    """
    CREATE TABLE dim_products_sink (
        old_id INT,
        name STRING,
        category STRING,
        price DECIMAL(10,2),
        weight DECIMAL(10,2),
        color STRING,
        size STRING,
        brand STRING,
        material STRING,
        description STRING,
        rating DECIMAL(2,1),
        reviews INT,
        release_date DATE,
        expiry_date DATE,
        pet_category STRING,
        PRIMARY KEY (old_id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/bigdata',
        'table-name' = 'dim_products',
        'username' = 'user',
        'password' = 'pass',
        'driver' = 'org.postgresql.Driver'
    )
    """
)
print("📄 Inserting data into dim_products_sink...")
t_env.execute_sql(
    """
    INSERT INTO dim_products_sink
    SELECT
        id,
        product_name,
        product_category,
        product_price,
        product_weight,
        product_color,
        product_size,
        product_brand,
        product_material,
        product_description,
        product_rating,
        product_reviews,
        TO_DATE(product_release_date),
        TO_DATE(product_expiry_date),
        pet_category
    FROM kafka_source
    """
)

# dim_stores_sink
print("🛠 Creating dim_stores_sink table...")
t_env.execute_sql(
    """
    CREATE TABLE dim_stores_sink (
        old_id INT,
        name STRING,
        location STRING,
        city STRING,
        state STRING,
        country STRING,
        phone STRING,
        email STRING,
        PRIMARY KEY (old_id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/bigdata',
        'table-name' = 'dim_stores',
        'username' = 'user',
        'password' = 'pass',
        'driver' = 'org.postgresql.Driver'
    )
    """
)
print("📄 Inserting data into dim_stores_sink...")
t_env.execute_sql(
    """
    INSERT INTO dim_stores_sink
    SELECT
        id,
        store_name,
        store_location,
        store_city,
        store_state,
        store_country,
        store_phone,
        store_email
    FROM kafka_source
    """
)

# dim_suppliers_sink
print("🛠 Creating dim_suppliers_sink table...")
t_env.execute_sql(
    """
    CREATE TABLE dim_suppliers_sink (
        old_id INT,
        name STRING,
        contact STRING,
        email STRING,
        phone STRING,
        address STRING,
        city STRING,
        country STRING,
        PRIMARY KEY (old_id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/bigdata',
        'table-name' = 'dim_suppliers',
        'username' = 'user',
        'password' = 'pass',
        'driver' = 'org.postgresql.Driver'
    )
    """
)
print("📄 Inserting data into dim_suppliers_sink...")
t_env.execute_sql(
    """
    INSERT INTO dim_suppliers_sink
    SELECT
        id,
        supplier_name,
        supplier_contact,
        supplier_email,
        supplier_phone,
        supplier_address,
        supplier_city,
        supplier_country
    FROM kafka_source
    """
)

# fact_sales_sink
print("🛠 Creating fact_sales_sink table...")
t_env.execute_sql(
    """
    CREATE TABLE fact_sales_sink (
        old_id INT,
        sale_date DATE,
        quantity INT,
        total_price DECIMAL(10,2),
        customer_id INT,
        seller_id INT,
        product_id INT,
        store_id INT,
        supplier_id INT,
        PRIMARY KEY (old_id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/bigdata',
        'table-name' = 'fact_sales',
        'username' = 'user',
        'password' = 'pass',
        'driver' = 'org.postgresql.Driver'
    )
    """
)
print("📄 Inserting data into fact_sales_sink...")
t_env.execute_sql(
    """
    INSERT INTO fact_sales_sink
    SELECT
        id,
        TO_DATE(sale_date, 'MM/DD/YYYY'),
        sale_quantity,
        sale_total_price,
        sale_customer_id,
        sale_seller_id,
        sale_product_id,
        id,
        id
    FROM kafka_source
    """
)

print("✅ ETL job completed")
