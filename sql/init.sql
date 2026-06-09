-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ==============================================================================
-- DIMENSION TABLES (DWH)
-- ==============================================================================

-- 1. dim_person
CREATE TABLE IF NOT EXISTS dim_person (
    person_id VARCHAR(50) PRIMARY KEY,
    person_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed dim_person
INSERT INTO dim_person (person_id, person_name) 
VALUES ('P1', 'Person 1')
ON CONFLICT (person_id) DO NOTHING;

-- 2. dim_activity
CREATE TABLE IF NOT EXISTS dim_activity (
    activity_id SERIAL PRIMARY KEY,
    activity_label VARCHAR(50) UNIQUE NOT NULL,
    activity_category VARCHAR(20) NOT NULL CHECK (activity_category IN ('sedentary', 'light', 'intense'))
);

-- Seed dim_activity (Mapped exactly to model labels in utils_spark.py)
INSERT INTO dim_activity (activity_label, activity_category) VALUES
    ('sitting', 'sedentary'),
    ('standing', 'sedentary'),
    ('lying_back', 'sedentary'),
    ('lying_right', 'sedentary'),
    ('standing_elevator', 'sedentary'),
    ('moving_elevator', 'light'),
    ('walking_treadmill', 'light'),
    ('descending_stairs', 'light'),
    ('ascending_stairs', 'intense'),
    ('running_treadmill', 'intense')
ON CONFLICT (activity_label) DO NOTHING;

-- 3. dim_time
CREATE TABLE IF NOT EXISTS dim_time (
    date_id INT PRIMARY KEY, -- Format: YYYYMMDD
    full_date DATE UNIQUE NOT NULL,
    day_name VARCHAR(20),
    month INT,
    month_name VARCHAR(20),
    quarter INT,
    year INT
);

-- Generate calendar dates from 2024 to 2030
INSERT INTO dim_time (date_id, full_date, day_name, month, month_name, quarter, year)
SELECT
    TO_CHAR(datum, 'YYYYMMDD')::INT AS date_id,
    datum::DATE AS full_date,
    TRIM(TO_CHAR(datum, 'Day')) AS day_name,
    EXTRACT(MONTH FROM datum)::INT AS month,
    TRIM(TO_CHAR(datum, 'Month')) AS month_name,
    EXTRACT(QUARTER FROM datum)::INT AS quarter,
    EXTRACT(YEAR FROM datum)::INT AS year
FROM generate_series('2024-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) AS datum
ON CONFLICT (date_id) DO NOTHING;

-- ==============================================================================
-- REALTIME / OPERATIONAL TABLES
-- ==============================================================================

-- 4. realtime_activity_monitor (Denormalized)
CREATE TABLE IF NOT EXISTS realtime_activity_monitor (
    event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    person_id VARCHAR(50) NOT NULL,
    sensor_event_time TIMESTAMP NOT NULL,
    predicted_at TIMESTAMP,
    stored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    predicted_activity_label VARCHAR(50),
    activity_category VARCHAR(20) CHECK (activity_category IN ('sedentary', 'light', 'intense') OR activity_category IS NULL),
    confidence_score DOUBLE PRECISION CHECK (confidence_score >= 0 AND confidence_score <= 1 OR confidence_score IS NULL),
    sedentary_start_time TIMESTAMP,
    sedentary_streak_sec DOUBLE PRECISION,
    sedentary_streak_min DOUBLE PRECISION,
    is_alert BOOLEAN DEFAULT FALSE,
    prediction_latency_sec DOUBLE PRECISION
);

-- Indexes for fast realtime dashboard polling
CREATE INDEX IF NOT EXISTS idx_realtime_person_id ON realtime_activity_monitor(person_id);
CREATE INDEX IF NOT EXISTS idx_realtime_event_time ON realtime_activity_monitor(sensor_event_time);
CREATE INDEX IF NOT EXISTS idx_realtime_person_time_desc ON realtime_activity_monitor(person_id, sensor_event_time DESC);
CREATE INDEX IF NOT EXISTS idx_realtime_is_alert ON realtime_activity_monitor(is_alert);

-- 5. activity_state (Stores latest state per person for calculating streaks in Spark)
CREATE TABLE IF NOT EXISTS activity_state (
    person_id VARCHAR(50) PRIMARY KEY,
    last_activity_label VARCHAR(50),
    last_activity_category VARCHAR(20) CHECK (last_activity_category IN ('sedentary', 'light', 'intense') OR last_activity_category IS NULL),
    sedentary_start_time TIMESTAMP,
    last_sensor_event_time TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==============================================================================
-- DATA WAREHOUSE FACT TABLES
-- ==============================================================================

-- 6. fact_activity_daily_summary
CREATE TABLE IF NOT EXISTS fact_activity_daily_summary (
    fact_id SERIAL PRIMARY KEY,
    date_id INT NOT NULL REFERENCES dim_time(date_id),
    person_id VARCHAR(50) NOT NULL REFERENCES dim_person(person_id),
    activity_id INT NOT NULL REFERENCES dim_activity(activity_id),
    total_duration_minutes DOUBLE PRECISION DEFAULT 0,
    avg_confidence_score DOUBLE PRECISION,
    alert_count INT DEFAULT 0,
    max_sedentary_streak_minutes DOUBLE PRECISION DEFAULT 0,
    prediction_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_daily_activity UNIQUE (date_id, person_id, activity_id)
);

-- Indexes for fact table queries
CREATE INDEX IF NOT EXISTS idx_fact_date_id ON fact_activity_daily_summary(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_person_id ON fact_activity_daily_summary(person_id);
CREATE INDEX IF NOT EXISTS idx_fact_activity_id ON fact_activity_daily_summary(activity_id);

-- ==============================================================================
-- MODEL MONITORING
-- ==============================================================================

-- 7. model_performance_metrics
CREATE TABLE IF NOT EXISTS model_performance_metrics (
    model_id SERIAL PRIMARY KEY,
    model_name VARCHAR(100),
    algorithm VARCHAR(100),
    accuracy DOUBLE PRECISION CHECK (accuracy >= 0 AND accuracy <= 1 OR accuracy IS NULL),
    precision_score DOUBLE PRECISION CHECK (precision_score >= 0 AND precision_score <= 1 OR precision_score IS NULL),
    recall_score DOUBLE PRECISION CHECK (recall_score >= 0 AND recall_score <= 1 OR recall_score IS NULL),
    f1_score DOUBLE PRECISION CHECK (f1_score >= 0 AND f1_score <= 1 OR f1_score IS NULL),
    trained_at TIMESTAMP,
    model_path TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
