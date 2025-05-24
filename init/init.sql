-- Создание таблиц измерений
CREATE TABLE dim_customers (
    customer_id SERIAL PRIMARY KEY,
    old_id INTEGER UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age INTEGER,
    email VARCHAR(100) UNIQUE,
    country VARCHAR(100),
    postal_code VARCHAR(20),
    pet_type VARCHAR(50),
    pet_name VARCHAR(100),
    pet_breed VARCHAR(100)
);

CREATE TABLE dim_sellers (
    seller_id SERIAL PRIMARY KEY,
    old_id INTEGER UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(100) UNIQUE,
    country VARCHAR(100),
    postal_code VARCHAR(20)
);

CREATE TABLE dim_products (
    product_id SERIAL PRIMARY KEY,
    old_id INTEGER UNIQUE,
    name VARCHAR(100),
    category VARCHAR(100),
    price DECIMAL(10, 2),
    weight DECIMAL(10, 2),
    color VARCHAR(50),
    size VARCHAR(20),
    brand VARCHAR(100),
    material VARCHAR(100),
    description TEXT,
    rating DECIMAL(2, 1),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE,
    pet_category VARCHAR(50)
);

CREATE TABLE dim_stores (
    store_id SERIAL PRIMARY KEY,
    old_id INTEGER UNIQUE,
    name VARCHAR(100),
    location VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    phone VARCHAR(20),
    email VARCHAR(100)
);

CREATE TABLE dim_suppliers (
    supplier_id SERIAL PRIMARY KEY,
    old_id INTEGER UNIQUE,
    name VARCHAR(100),
    contact VARCHAR(100),
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    country VARCHAR(100)
);

-- Фактическая таблица продаж
CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    old_id INTEGER UNIQUE,
    sale_date DATE,
    quantity INTEGER,
    total_price DECIMAL(10, 2),
    customer_id INTEGER,
    seller_id INTEGER,
    product_id INTEGER,
    store_id INTEGER,
    supplier_id INTEGER
);
