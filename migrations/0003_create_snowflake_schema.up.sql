CREATE TABLE IF NOT EXISTS dim_pet_breed (
    pet_breed_id SERIAL PRIMARY KEY,
    breed_name   VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id    INTEGER PRIMARY KEY,
    first_name     VARCHAR(100),
    last_name      VARCHAR(100),
    age            INTEGER,
    email          VARCHAR(200),
    country        VARCHAR(100),
    postal_code    VARCHAR(50),
    pet_type       VARCHAR(50),
    pet_name       VARCHAR(100),
    pet_breed_id   INTEGER REFERENCES dim_pet_breed(pet_breed_id)
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id   INTEGER PRIMARY KEY,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    email       VARCHAR(200) UNIQUE,
    country     VARCHAR(100),
    postal_code VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    name        VARCHAR(200),
    contact     VARCHAR(200),
    email       VARCHAR(200),
    phone       VARCHAR(50),
    address     VARCHAR(200),
    city        VARCHAR(100),
    country     VARCHAR(100),
    UNIQUE(name, contact)
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id    SERIAL PRIMARY KEY,
    name        VARCHAR(200),
    address     VARCHAR(200),
    city        VARCHAR(100),
    state       VARCHAR(100),
    country     VARCHAR(100),
    phone       VARCHAR(50),
    email       VARCHAR(200),
    UNIQUE(name, city, state)
);

CREATE TABLE IF NOT EXISTS dim_pet_category (
    pet_category_id SERIAL PRIMARY KEY,
    category_name   VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id          INTEGER PRIMARY KEY,
    name                VARCHAR(200),
    category            VARCHAR(100),
    price               NUMERIC(10,2),
    quantity            INTEGER,
    weight              NUMERIC(10,2),
    color               VARCHAR(50),
    size                VARCHAR(50),
    brand               VARCHAR(100),
    material            VARCHAR(100),
    description         TEXT,
    rating              NUMERIC(3,1),
    reviews             INTEGER,
    release_date        DATE,
    expiry_date         DATE,
    pet_category_id     INTEGER REFERENCES dim_pet_category(pet_category_id)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id     INTEGER PRIMARY KEY,
    full_date   DATE NOT NULL UNIQUE,
    day         INTEGER,
    month       INTEGER,
    year        INTEGER,
    quarter     INTEGER
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id         SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES dim_customer(customer_id),
    seller_id       INTEGER REFERENCES dim_seller(seller_id),
    product_id      INTEGER REFERENCES dim_product(product_id),
    store_id        INTEGER REFERENCES dim_store(store_id),
    supplier_id     INTEGER REFERENCES dim_supplier(supplier_id),
    date_id         INTEGER REFERENCES dim_date(date_id),
    sale_quantity   INTEGER,
    sale_total_price NUMERIC(10,2)
);
