-- Criação de esquema dedicado para o modelo dimensional
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'cafes_urbanos')
BEGIN
    EXEC('CREATE SCHEMA cafes_urbanos');
END
GO

-- Dimensão de clientes
CREATE TABLE IF NOT EXISTS cafes_urbanos.dim_customer (
    customer_sk INT IDENTITY(1,1) PRIMARY KEY,
    customer_code NVARCHAR(10) NOT NULL,
    full_name NVARCHAR(120) NOT NULL,
    email NVARCHAR(150) NULL,
    city NVARCHAR(60) NULL,
    state NVARCHAR(2) NULL,
    loyalty_tier NVARCHAR(20) NULL,
    signup_date DATE NULL
)
WITH (DISTRIBUTION = REPLICATE, HEAP);
GO

-- Dimensão de produtos
CREATE TABLE IF NOT EXISTS cafes_urbanos.dim_product (
    product_sk INT IDENTITY(1,1) PRIMARY KEY,
    product_code NVARCHAR(10) NOT NULL,
    product_name NVARCHAR(120) NOT NULL,
    category NVARCHAR(60) NULL,
    sub_category NVARCHAR(60) NULL,
    brand NVARCHAR(60) NULL,
    unit_cost DECIMAL(10,2) NULL,
    premium_flag BIT NOT NULL DEFAULT 0
)
WITH (DISTRIBUTION = REPLICATE, HEAP);
GO

-- Dimensão de lojas
CREATE TABLE IF NOT EXISTS cafes_urbanos.dim_store (
    store_sk INT IDENTITY(1,1) PRIMARY KEY,
    store_code NVARCHAR(10) NOT NULL,
    store_name NVARCHAR(120) NOT NULL,
    city NVARCHAR(60) NULL,
    state NVARCHAR(2) NULL,
    region NVARCHAR(30) NULL,
    store_format NVARCHAR(40) NULL,
    opening_date DATE NULL
)
WITH (DISTRIBUTION = REPLICATE, HEAP);
GO

-- Dimensão de datas
CREATE TABLE IF NOT EXISTS cafes_urbanos.dim_date (
    date_sk INT IDENTITY(1,1) PRIMARY KEY,
    date_key INT NOT NULL,
    full_date DATE NOT NULL,
    day TINYINT NOT NULL,
    month TINYINT NOT NULL,
    month_name NVARCHAR(20) NOT NULL,
    quarter TINYINT NOT NULL,
    year SMALLINT NOT NULL,
    is_weekend BIT NOT NULL
)
WITH (DISTRIBUTION = REPLICATE, HEAP);
GO

-- Tabela fato
CREATE TABLE IF NOT EXISTS cafes_urbanos.fact_sales (
    sale_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    date_sk INT NOT NULL,
    store_sk INT NOT NULL,
    product_sk INT NOT NULL,
    customer_sk INT NOT NULL,
    order_datetime DATETIME2 NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount DECIMAL(10,2) NOT NULL,
    gross_amount DECIMAL(12,2) NOT NULL,
    net_amount DECIMAL(12,2) NOT NULL,
    payment_type NVARCHAR(40) NULL,
    channel NVARCHAR(40) NULL
)
WITH (DISTRIBUTION = HASH(store_sk), CLUSTERED COLUMNSTORE INDEX);
GO

-- Criação de visão para análise diária por canal
drop view if exists cafes_urbanos.vw_sales_daily_channel;
GO
CREATE VIEW cafes_urbanos.vw_sales_daily_channel AS
SELECT
    d.full_date,
    f.channel,
    SUM(f.net_amount) AS total_net_amount,
    SUM(f.quantity) AS total_quantity,
    COUNT(DISTINCT f.sale_sk) AS transactions
FROM cafes_urbanos.fact_sales f
JOIN cafes_urbanos.dim_date d ON d.date_sk = f.date_sk
GROUP BY d.full_date, f.channel;
GO
