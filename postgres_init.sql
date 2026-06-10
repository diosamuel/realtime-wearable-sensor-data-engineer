-- Dimension Tables
CREATE TABLE IF NOT EXISTS dim_person (
    person_id INT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    gender VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS dim_activity (
    activity_id VARCHAR(10) PRIMARY KEY,
    activity_label VARCHAR(50),
    activity_category VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_key SERIAL PRIMARY KEY,
    full_date DATE,
    day INT,
    month INT,
    year INT,
    day_name VARCHAR(20)
);

-- Operational Table (Real-time)
CREATE TABLE IF NOT EXISTS realtime_activity_monitor (
    user_id INT,
    predicted_activity_label VARCHAR(50),
    activity_category VARCHAR(50),
    confidence_score DOUBLE PRECISION,
    sedentary_duration_minutes DOUBLE PRECISION,
    alert_status VARCHAR(20),
    processing_latency_sec DOUBLE PRECISION,
    sensor_event_time TIMESTAMP,
    predicted_at TIMESTAMP,
    stored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Stateful Tracking (Internal use)
CREATE TABLE IF NOT EXISTS activity_state (
    user_id INT PRIMARY KEY,
    last_activity_category VARCHAR(50),
    sedentary_start_time TIMESTAMP,
    sedentary_streak_minutes DOUBLE PRECISION
);

-- Fact Table (Historical)
CREATE TABLE IF NOT EXISTS fact_activity_daily_summary (
    user_id INT,
    date DATE,
    activity_label VARCHAR(50),
    total_duration_minutes DOUBLE PRECISION,
    avg_confidence_score DOUBLE PRECISION,
    alert_count INT,
    max_sedentary_streak_minutes DOUBLE PRECISION,
    prediction_count INT,
    PRIMARY KEY (user_id, date, activity_label)
);

-- Initial Data for Dimensions
INSERT INTO dim_activity (activity_id, activity_label, activity_category) VALUES
('a01', 'sitting', 'sedentary'),
('a02', 'standing', 'sedentary'),
('a03', 'lying_back', 'sedentary'),
('a04', 'lying_right', 'sedentary'),
('a07', 'standing_elevator', 'sedentary'),
('a05', 'ascending_stairs', 'light'),
('a06', 'descending_stairs', 'light'),
('a08', 'moving_elevator', 'light'),
('a10', 'walking_treadmill', 'light'),
('a12', 'running_treadmill', 'intense')
ON CONFLICT (activity_id) DO NOTHING;

-- Initial State for User 1
INSERT INTO activity_state (user_id, last_activity_category, sedentary_start_time, sedentary_streak_minutes)
VALUES (1, 'unknown', NULL, 0)
ON CONFLICT (user_id) DO NOTHING;
