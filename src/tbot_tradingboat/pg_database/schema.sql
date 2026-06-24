-- TradingBoat PostgreSQL schema
--
-- Mirrors the SQLAlchemy DDL created at runtime by setup_connection() in
-- orderdb.py / alertdb.py / errordb.py. Provided for manual setup/review,
-- e.g. `psql -h <host> -U <user> -d <dbname> -f schema.sql`.
--
-- Note: uniquekey / tv_timestamp are formatted strings
-- ("YYYY-MM-DD HH:MM:SS.fff", see get_timestamp() in tbot_api.py), not
-- native TIMESTAMP columns -- they sort lexically the same as chronologically.

CREATE TABLE IF NOT EXISTS tbotorders (
    id              BIGSERIAL PRIMARY KEY,
    timestamp       TIMESTAMP NOT NULL DEFAULT NOW(),
    uniquekey       VARCHAR(32),
    tv_price        DOUBLE PRECISION,
    orderid         INTEGER,
    ticker          VARCHAR(32),
    action          VARCHAR(16),
    ordertype       VARCHAR(16),
    lmtprice        DOUBLE PRECISION,
    auxprice        DOUBLE PRECISION,
    qty             DOUBLE PRECISION,
    avgfillprice    DOUBLE PRECISION,
    orderstatus     VARCHAR(32),
    orderref        VARCHAR(64),
    parentid        INTEGER,
    position        DOUBLE PRECISION,
    mrkvalue        DOUBLE PRECISION,
    avgprice        DOUBLE PRECISION,
    unrealizedpnl   DOUBLE PRECISION,
    realizedpnl     DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_tbotorders_ticker_orderref ON tbotorders (ticker, orderref);

CREATE TABLE IF NOT EXISTS tbotalerts (
    id              BIGSERIAL PRIMARY KEY,
    timestamp       TIMESTAMP NOT NULL DEFAULT NOW(),
    uniquekey       VARCHAR(32),
    tv_timestamp    VARCHAR(32),
    ticker          VARCHAR(32),
    direction       VARCHAR(32),
    timeframe       VARCHAR(16),
    qty             DOUBLE PRECISION,
    orderref        VARCHAR(64),
    alertstatus     VARCHAR(32),
    entrylimit      DOUBLE PRECISION,
    entrystop       DOUBLE PRECISION,
    exitlimit       DOUBLE PRECISION,
    exitstop        DOUBLE PRECISION,
    tv_price        DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS alert_index ON tbotalerts (uniquekey);

CREATE TABLE IF NOT EXISTS tboterrors (
    id              BIGSERIAL PRIMARY KEY,
    timestamp       TIMESTAMP NOT NULL DEFAULT NOW(),
    reqid           DOUBLE PRECISION,
    errcode         INTEGER,
    symbol          VARCHAR(32),
    errstr          TEXT
);

-- Retention: keep only the most recent `max_records` rows (by `key_col`),
-- sampled every 64 inserts. Mirrors the old SQLite per-table trigger.

CREATE OR REPLACE FUNCTION trig_trim_tbotorders() RETURNS trigger AS $$
BEGIN
    IF NEW.id % 64 = 0 THEN
        DELETE FROM tbotorders WHERE uniquekey NOT IN (
            SELECT uniquekey FROM tbotorders ORDER BY uniquekey DESC LIMIT 3600
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS trig_tbotorders ON tbotorders;
CREATE TRIGGER trig_tbotorders AFTER INSERT ON tbotorders
    FOR EACH ROW EXECUTE FUNCTION trig_trim_tbotorders();

CREATE OR REPLACE FUNCTION trig_trim_tbotalerts() RETURNS trigger AS $$
BEGIN
    IF NEW.id % 64 = 0 THEN
        DELETE FROM tbotalerts WHERE uniquekey NOT IN (
            SELECT uniquekey FROM tbotalerts ORDER BY uniquekey DESC LIMIT 3600
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS trig_tbotalerts ON tbotalerts;
CREATE TRIGGER trig_tbotalerts AFTER INSERT ON tbotalerts
    FOR EACH ROW EXECUTE FUNCTION trig_trim_tbotalerts();

CREATE OR REPLACE FUNCTION trig_trim_tboterrors() RETURNS trigger AS $$
BEGIN
    IF NEW.id % 64 = 0 THEN
        DELETE FROM tboterrors WHERE timestamp NOT IN (
            SELECT timestamp FROM tboterrors ORDER BY timestamp DESC LIMIT 3600
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS trig_tboterrors ON tboterrors;
CREATE TRIGGER trig_tboterrors AFTER INSERT ON tboterrors
    FOR EACH ROW EXECUTE FUNCTION trig_trim_tboterrors();
