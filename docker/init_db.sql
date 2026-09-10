-- =============================================================================
-- Smart Grid Energy Monitoring System: Database Initialization Script
-- Architecture: Kappa Stream Processing Serving Layer
-- Target DB: PostgreSQL 14+
-- =============================================================================

-- Ensure database exists (handled during container initialization)
-- CREATE DATABASE smart_grid_db;

\connect smart_grid_db;

-- 1. Aggregated Grid Zone Metrics (Sink Table for PySpark Structured Streaming)
-- Stores windowed aggregations of power consumption, solar generation, net load,
-- renewable penetration, and billing cost estimates by geographical grid zone.
CREATE TABLE IF NOT EXISTS grid_zone_metrics (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    grid_zone VARCHAR(50) NOT NULL,
    total_consumption_kwh NUMERIC(14, 4) NOT NULL,
    total_solar_generation_kwh NUMERIC(14, 4) NOT NULL,
    net_load_kwh NUMERIC(14, 4) NOT NULL,
    renewable_contribution_pct NUMERIC(6, 2) NOT NULL,
    total_cost_estimate NUMERIC(14, 4) NOT NULL,
    active_meters INT NOT NULL,
    batch_id BIGINT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_grid_zone_metrics PRIMARY KEY (window_start, window_end, grid_zone)
);

-- Fast lookup index for real-time dashboards filtering by zone and window recency
CREATE INDEX IF NOT EXISTS idx_grid_zone_window 
ON grid_zone_metrics (grid_zone, window_start DESC);

-- 2. Pipeline Health & Heartbeat Logs (Populated by Airflow Health Monitor)
-- Records periodic health audits, broker connectivity, watermarking latency, and SLA compliance.
CREATE TABLE IF NOT EXISTS pipeline_health_logs (
    id SERIAL PRIMARY KEY,
    component VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'HEALTHY', 'DEGRADED', 'UNHEALTHY'
    latency_ms NUMERIC(10, 2),
    details JSONB,
    checked_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_health_checked 
ON pipeline_health_logs (checked_at DESC);

-- 3. Tariffs Historical Audit Table (Optional Historical Batch Archive)
-- Tracks the evolution of daily static tariff drops over simulated days.
CREATE TABLE IF NOT EXISTS tariffs_history (
    id SERIAL PRIMARY KEY,
    household_id VARCHAR(50) NOT NULL,
    tariff_rate NUMERIC(8, 4) NOT NULL,
    billing_tier VARCHAR(50) NOT NULL,
    subsidy_flag SMALLINT NOT NULL,
    simulated_day_date DATE NOT NULL,
    effective_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tariffs_history_household 
ON tariffs_history (household_id, simulated_day_date);

-- Grant privileges (default postgres user has full control)
COMMENT ON TABLE grid_zone_metrics IS 'Stores micro-batch aggregated smart grid metrics by zone and time window computed via PySpark Structured Streaming.';
COMMENT ON TABLE pipeline_health_logs IS 'Audit log of component heartbeats, latencies, and pipeline health verified by Airflow.';
COMMENT ON TABLE tariffs_history IS 'Historical snapshots of daily tariff rate drops.';
