# Data Contract: Sensor Activity Architecture Phase 1

## 1. Overview
This project processes high-frequency wearable sensor data to predict human activities in real-time. To support both low-latency operational monitoring and analytical historical reporting, we are explicitly separating the database schema into two distinct paths:

*   **Realtime Table (`realtime_activity_monitor`)**: A highly denormalized, operational table optimized for fast, single-row inserts and simple polling. It does *not* follow a star schema to avoid costly JOINs during real-time dashboard rendering.
*   **Data Warehouse (DWH) Fact Table (`fact_activity_daily_summary`)**: A pre-aggregated table following a standard dimensional model (star schema) optimized for historical analytics, trend spotting, and complex queries over time.

## 2. Dashboard Data Source Mapping

| Dashboard Page | Target Table | Description |
| :--- | :--- | :--- |
| **Page 1: Real-Time Activity Monitor** | `realtime_activity_monitor` | Displays the current live status, prediction latency, and active sedentary alerts for a specific person. No dimensional JOINs required. |
| **Page 2: Activity Analytics** | `fact_activity_daily_summary` (JOINed with dimensions) | Displays historical trends, aggregated durations per activity, and comparisons across users. |
| **Page 3: Model Performance** | `model_performance_metrics` | Displays evaluation metrics of the deployed models to track drift or accuracy degradation. |

---

## 3. Realtime Table Contract
**Table Name:** `realtime_activity_monitor`
**Grain:** `1 row = 1 prediction result for 1 person at 1 sensor_event_time`

*Note: This is an operational/streaming table, not the primary DWH fact table.*

| Column Name | Recommended Data Type | Description | Example Value |
| :--- | :--- | :--- | :--- |
| `event_id` | `UUID` | Unique identifier for the prediction event. | `550e8400-e29b-41d4-a716-446655440000` |
| `person_id` | `INT` | ID of the person wearing the sensor. | `7` |
| `sensor_event_time` | `TIMESTAMP` | The exact time the sensor recorded the data (Epoch generated at sensor/simulation). | `2024-05-20 14:30:00.000` |
| `predicted_at` | `TIMESTAMP` | The time Spark MLlib finished calculating the prediction. | `2024-05-20 14:30:02.150` |
| `stored_at` | `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` | The time the row was inserted into PostgreSQL. | `2024-05-20 14:30:02.300` |
| `predicted_activity_label` | `VARCHAR(50)` | The specific activity predicted by the model. | `sitting` |
| `activity_category` | `VARCHAR(20)` | Broader category mapped from the label (sedentary, light, intense). | `sedentary` |
| `confidence_score` | `FLOAT` | The highest probability score from the Random Forest model. | `0.87` |
| `sedentary_start_time` | `TIMESTAMP` | The `sensor_event_time` when the current sedentary streak began. NULL if not sedentary. | `2024-05-20 14:00:00.000` |
| `sedentary_streak_sec` | `INT` | Duration in seconds of the current sedentary streak. | `1800` |
| `sedentary_streak_min` | `FLOAT` | Duration in minutes of the current sedentary streak (derived). | `30.0` |
| `is_alert` | `BOOLEAN` | TRUE if `sedentary_streak_sec` >= threshold. | `TRUE` |
| `prediction_latency_sec` | `FLOAT` | Difference in seconds between `predicted_at` and `sensor_event_time`. | `2.15` |

---

## 4. Sedentary Alert Logic
To ensure accuracy despite processing delays, the alert logic is tied strictly to the `sensor_event_time`.

**Core Rules:**
*   **Time Reference:** The streak is calculated based on `sensor_event_time`. `stored_at` and `predicted_at` are for system monitoring only.
*   **Initialization:** If `activity_category == "sedentary"` and the previous state was NOT sedentary, set `sedentary_start_time = sensor_event_time`.
*   **Continuation:** If the state remains `sedentary`, calculate `sedentary_streak_sec = current sensor_event_time - sedentary_start_time`.
*   **Reset:** If `activity_category` changes to `light` or `intense`, set `sedentary_start_time = NULL` and `sedentary_streak_sec = 0`.
*   **Alert Trigger:** `is_alert = TRUE` when `sedentary_streak_sec >= threshold_sec`.
    *   *Demo Threshold:* 50 seconds.
    *   *Production Threshold:* 1800 seconds (30 minutes).

**Pseudo-code Example:**
```python
if current_category == "sedentary":
    if previous_category != "sedentary":
        sedentary_start_time = current_sensor_event_time
    
    sedentary_streak_sec = (current_sensor_event_time - sedentary_start_time).total_seconds()
    is_alert = sedentary_streak_sec >= THRESHOLD_SEC
else:
    sedentary_start_time = NULL
    sedentary_streak_sec = 0
    is_alert = FALSE
```

---

## 5. Activity Category Mapping
Mapping the model's granular `activity_label` to broader categories for alerting.
*(TODO: Confirm final activity mapping against all model label indices. Current assumption based on UCI dataset labels).*

| `activity_label` (Model Output) | `activity_category` |
| :--- | :--- |
| `sitting` | `sedentary` |
| `standing` | `sedentary` |
| `lying_back` | `sedentary` |
| `lying_right` | `sedentary` |
| `standing_elevator` | `sedentary` |
| `walking_treadmill` | `light` |
| `moving_elevator` | `light` |
| `descending_stairs` | `light` |
| `ascending_stairs` | `intense` |
| `running_treadmill` | `intense` |

---

## 6. Data Warehouse Fact Table Contract
**Table Name:** `fact_activity_daily_summary`
**Grain:** `1 row = 1 person per date per activity_id`

*Note: This table is aggregated periodically (e.g., via Airflow batch jobs) from the operational data, not directly from the streaming pipeline.*

| Column Name | Recommended Data Type | Description |
| :--- | :--- | :--- |
| `fact_id` | `SERIAL / BIGINT` | Primary Key. |
| `date_id` | `INT` | Foreign Key to `dim_time` (Format: YYYYMMDD). |
| `person_id` | `INT` | Foreign Key to `dim_person`. |
| `activity_id` | `INT` | Foreign Key to `dim_activity`. |
| `total_duration_minutes` | `FLOAT` | Total time spent doing this activity on this date. |
| `avg_confidence_score` | `FLOAT` | Average model confidence for this activity prediction on this date. |
| `alert_count` | `INT` | Total number of times an alert was triggered for this activity (mostly applicable to sedentary). |
| `max_sedentary_streak_minutes` | `FLOAT` | The longest continuous streak for this activity on this date. |
| `prediction_count` | `INT` | Total number of predictions made for this activity on this date. |

---

## 7. Dimension Table Contract

| Table | Grain | Minimal Columns |
| :--- | :--- | :--- |
| `dim_person` | 1 row = 1 unique user/subject | `person_id` (PK), `name`, `age`, `weight_kg`, `height_cm` |
| `dim_activity` | 1 row = 1 unique activity type | `activity_id` (PK), `activity_label` (e.g., walking), `category` (e.g., light) |
| `dim_time` | 1 row = 1 calendar day | `date_id` (PK, YYYYMMDD), `full_date` (DATE), `day_name`, `is_weekend` |

---

## 8. Model Performance Contract
**Table Name:** `model_performance_metrics`
**Grain:** `1 row = 1 model evaluation run`

| Column Name | Recommended Data Type | Description |
| :--- | :--- | :--- |
| `model_id` | `VARCHAR(50)` | Primary Key (e.g., `rf_v1.0_20240520`). |
| `model_name` | `VARCHAR(100)` | Name of the model. |
| `algorithm` | `VARCHAR(50)` | e.g., `RandomForestClassifier`. |
| `accuracy` | `FLOAT` | Overall evaluation accuracy on the test set. |
| `precision_score` | `FLOAT` | Weighted precision score. |
| `recall_score` | `FLOAT` | Weighted recall score. |
| `f1_score` | `FLOAT` | Weighted F1 score. |
| `trained_at` | `TIMESTAMP` | Timestamp when training completed. |
| `model_path` | `VARCHAR(255)` | S3/MinIO URI where the model artifact is stored. |

---

## 9. Out of Scope Phase 1
The following technical implementations are **not** part of this Phase 1 contract and remain unbuilt:
*   Writing DDL SQL files (`init.sql`).
*   Modifying the Spark streaming script (`kafka_sensor_stream.py`) to run inference or write to PostgreSQL via JDBC.
*   Building the actual Tableau/Streamlit dashboard UI.
*   Implementing the MQTT-to-Kafka bridge.

## 10. Next Phase (Phase 2)
The next phase will involve technical implementation of this contract, starting with creating the `sql/init.sql` file to instantiate the `realtime_activity_monitor` and DWH schema in PostgreSQL, followed by adapting the data generation and streaming scripts to emit the required timestamps.