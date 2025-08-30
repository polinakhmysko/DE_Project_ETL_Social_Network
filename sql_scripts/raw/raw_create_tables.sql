-- Пользователи
CREATE TABLE IF NOT EXISTS raw.users (
    user_id UUID,
    name TEXT,
    email TEXT,
    registered_at TIMESTAMP,
    age INT,
    gender TEXT,
    device_type TEXT,
    os TEXT,
    ip_address INET,
    country TEXT
);

-- Сессии пользователей
CREATE TABLE IF NOT EXISTS raw.sessions (
    session_id UUID,
    user_id UUID,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    pages_viewed INT
);

-- События пользователей
CREATE TABLE IF NOT EXISTS raw.events (
    event_id UUID,
    session_id UUID,
    user_id UUID,
    event_type TEXT,
    timestamp TIMESTAMP
);

-- Заказы
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id UUID,
    user_id UUID,
    created_at TIMESTAMP,
    category TEXT,
    product TEXT,
    quantity INT,
    price NUMERIC(10,2),
    total_price NUMERIC(12,2),
    payment_method TEXT,
    supplier TEXT,
    supplier_country TEXT
);

-- Маркетинговые кампании
CREATE TABLE IF NOT EXISTS raw.campaigns (
    campaign_id UUID,
    name TEXT,
    start_date DATE,
    end_date DATE,
    promocode TEXT,
    discount_percent INT
);

-- Пользователи в кампаниях
CREATE TABLE IF NOT EXISTS raw.userCampaigns (
    user_id UUID,
    campaign_id UUID
);

-- Таблица обработанных файлов из MinIO
CREATE TABLE IF NOT EXISTS raw.processed_files (
    file_key TEXT PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT now()
);
