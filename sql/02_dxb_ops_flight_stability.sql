-- =========================================================
-- 02_dxb_ops_flight_stability.sql  (fixed for psql/Jupyter)
-- Membangun SCD2-style history + fact_flight_stability
-- =========================================================

-- =========================================
-- A. SCHEMA & TABLES
-- =========================================

CREATE SCHEMA IF NOT EXISTS dxb_ops;

-- bersihkan dulu supaya repeatable
DROP TABLE IF EXISTS dxb_ops.fact_flight_stability;
DROP TABLE IF EXISTS dxb_ops.flights_history;
DROP TABLE IF EXISTS dxb_ops.flights_live;

-- Tabel "live" (kondisi terkini per flight)
CREATE TABLE dxb_ops.flights_live (
    flight_id       BIGSERIAL PRIMARY KEY,
    flight_number   VARCHAR(10) NOT NULL,
    op_date         DATE        NOT NULL,  -- tanggal operasi di DXB
    airline_code    VARCHAR(5)  NOT NULL,
    origin          VARCHAR(10) NOT NULL,
    destination     VARCHAR(10) NOT NULL,

    std             TIMESTAMP,  -- scheduled departure DXB
    sta             TIMESTAMP,  -- scheduled arrival DXB
    etd             TIMESTAMP,  -- estimated departure
    eta             TIMESTAMP,  -- estimated arrival
    atd             TIMESTAMP,  -- actual departure
    ata             TIMESTAMP,  -- actual arrival

    -- contoh nilai: SCHED / BOARDING / DEPARTED / ARRIVED / CANCELLED
    status_code     VARCHAR(20),
    gate            VARCHAR(10),
    stand           VARCHAR(10),
    terminal_code   VARCHAR(5),

    last_update_ts  TIMESTAMP NOT NULL DEFAULT now()
);

-- Tabel history (semua perubahan pada flight)
CREATE TABLE dxb_ops.flights_history (
    history_id      BIGSERIAL PRIMARY KEY,
    flight_id       BIGINT NOT NULL REFERENCES dxb_ops.flights_live(flight_id),

    flight_number   VARCHAR(10) NOT NULL,
    op_date         DATE        NOT NULL,
    airline_code    VARCHAR(5)  NOT NULL,
    origin          VARCHAR(10) NOT NULL,
    destination     VARCHAR(10) NOT NULL,

    std             TIMESTAMP,
    sta             TIMESTAMP,
    etd             TIMESTAMP,
    eta             TIMESTAMP,
    atd             TIMESTAMP,
    ata             TIMESTAMP,
    status_code     VARCHAR(20),
    gate            VARCHAR(10),
    stand           VARCHAR(10),
    terminal_code   VARCHAR(5),

    valid_from      TIMESTAMP NOT NULL,
    valid_to        TIMESTAMP,
    change_reason   VARCHAR(50),
    changed_by      VARCHAR(50)
);

CREATE INDEX idx_flights_history_flight_validfrom
ON dxb_ops.flights_history (flight_id, valid_from);

-- =========================================
-- B. TRIGGER UNTUK MENGISI HISTORY
-- =========================================

CREATE OR REPLACE FUNCTION dxb_ops.flights_live_audit()
RETURNS trigger AS
$$
DECLARE
    v_change_reason text;
BEGIN
    IF TG_OP = 'INSERT' THEN
        v_change_reason := 'INSERT';
    ELSE
        -- deteksi jenis perubahan saat UPDATE
        IF NEW.etd IS DISTINCT FROM OLD.etd THEN
            v_change_reason := 'ETD_CHANGE';
        ELSIF NEW.status_code IS DISTINCT FROM OLD.status_code THEN
            v_change_reason := 'STATUS_CHANGE';
        ELSIF NEW.gate IS DISTINCT FROM OLD.gate THEN
            v_change_reason := 'GATE_CHANGE';
        ELSE
            v_change_reason := 'OTHER_UPDATE';
        END IF;

        -- tutup snapshot lama
        UPDATE dxb_ops.flights_history
        SET valid_to = now()
        WHERE flight_id = OLD.flight_id
          AND valid_to IS NULL;
    END IF;

    -- snapshot baru
    INSERT INTO dxb_ops.flights_history (
        flight_id, flight_number, op_date, airline_code, origin, destination,
        std, sta, etd, eta, atd, ata, status_code, gate, stand, terminal_code,
        valid_from, valid_to, change_reason, changed_by
    )
    VALUES (
        NEW.flight_id, NEW.flight_number, NEW.op_date, NEW.airline_code,
        NEW.origin, NEW.destination,
        NEW.std, NEW.sta, NEW.etd, NEW.eta, NEW.atd, NEW.ata,
        NEW.status_code, NEW.gate, NEW.stand, NEW.terminal_code,
        now(), NULL, v_change_reason, current_user
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_flights_live_audit ON dxb_ops.flights_live;

CREATE TRIGGER trg_flights_live_audit
AFTER INSERT OR UPDATE ON dxb_ops.flights_live
FOR EACH ROW
EXECUTE FUNCTION dxb_ops.flights_live_audit();

-- =========================================
-- C. SEED DATA: AMBIL DARI public.flights
--    (dibuat oleh 01_public_dxb_ops.sql)
-- =========================================

INSERT INTO dxb_ops.flights_live (
    flight_number, op_date, airline_code, origin, destination,
    std, sta, etd, eta, atd, ata,
    status_code, gate, stand, terminal_code
)
SELECT
    f.flight_number,
    DATE(COALESCE(f.std, f.sta)) AS op_date,
    f.airline_code,
    f.origin,
    f.destination,
    f.std,
    f.sta,
    f.std AS etd,
    f.sta AS eta,
    f.atd,
    f.ata,
    CASE WHEN f.atd IS NULL THEN 'SCHED' ELSE 'DEPARTED' END AS status_code,
    NULL AS gate,
    f.stand,
    f.terminal_code
FROM public.flights f;

-- Pada titik ini:
--  - dxb_ops.flights_live terisi 6 flight
--  - trigger sudah membuat 1 baris history per flight

-- =========================================
-- D. SIMULASI PERUBAHAN (BIAR ADA SNAPSHOT EXTRA)
-- =========================================

-- Contoh: BA108 delay 20 menit -> status DELAYED
UPDATE dxb_ops.flights_live
SET etd = etd + INTERVAL '20 minutes',
    status_code = 'DELAYED',
    last_update_ts = now()
WHERE flight_number = 'BA108';

-- Lalu boarding, ETD mundur lagi 10 menit -> status BOARDING
UPDATE dxb_ops.flights_live
SET etd = etd + INTERVAL '10 minutes',
    status_code = 'BOARDING',
    last_update_ts = now()
WHERE flight_number = 'BA108';

-- =========================================
-- E. HITUNG JUMLAH PERUBAHAN ETD & TOTAL SHIFT
-- =========================================

WITH ordered AS (
    SELECT
        h.flight_id,
        h.op_date,
        h.airline_code,
        h.etd,
        h.valid_from,
        LAG(h.etd) OVER (
            PARTITION BY h.flight_id
            ORDER BY h.valid_from
        ) AS prev_etd
    FROM dxb_ops.flights_history h
),
per_flight AS (
    SELECT
        o.flight_id,
        MIN(o.op_date)      AS op_date,
        MIN(o.airline_code) AS airline_code,
        COUNT(*) FILTER (
            WHERE o.prev_etd IS NOT NULL
              AND o.etd IS DISTINCT FROM o.prev_etd
        ) AS etd_change_count,
        EXTRACT(
            EPOCH FROM (
                MAX(o.etd) FILTER (WHERE o.etd IS NOT NULL)
              - MIN(o.etd) FILTER (WHERE o.etd IS NOT NULL)
            )
        ) / 60 AS total_etd_shift_min
    FROM ordered o
    GROUP BY o.flight_id
)
SELECT *
FROM per_flight
ORDER BY flight_id;

-- =========================================
-- F. FACT TABLE: dxb_ops.fact_flight_stability
-- =========================================

CREATE TABLE dxb_ops.fact_flight_stability (
    op_date             DATE        NOT NULL,
    flight_number       VARCHAR(10) NOT NULL,
    airline_code        VARCHAR(5)  NOT NULL,
    etd_change_count    INT         NOT NULL,
    total_etd_shift_min NUMERIC     NOT NULL,
    stability_score     NUMERIC     NOT NULL,
    PRIMARY KEY (op_date, flight_number)
);

-- refresh isi fact dari flights_history
TRUNCATE TABLE dxb_ops.fact_flight_stability;

WITH ordered AS (
    SELECT
        h.flight_id,
        h.op_date,
        h.airline_code,
        h.etd,
        h.valid_from,
        LAG(h.etd) OVER (
            PARTITION BY h.flight_id
            ORDER BY h.valid_from
        ) AS prev_etd
    FROM dxb_ops.flights_history h
),
per_flight AS (
    SELECT
        o.flight_id,
        MIN(o.op_date)      AS op_date,
        MIN(o.airline_code) AS airline_code,
        COUNT(*) FILTER (
            WHERE o.prev_etd IS NOT NULL
              AND o.etd IS DISTINCT FROM o.prev_etd
        ) AS etd_change_count,
        EXTRACT(
            EPOCH FROM (
                MAX(o.etd) FILTER (WHERE o.etd IS NOT NULL)
              - MIN(o.etd) FILTER (WHERE o.etd IS NOT NULL)
            )
        ) / 60 AS total_etd_shift_min
    FROM ordered o
    GROUP BY o.flight_id
)
INSERT INTO dxb_ops.fact_flight_stability (
    op_date,
    flight_number,
    airline_code,
    etd_change_count,
    total_etd_shift_min,
    stability_score
)
SELECT
    pf.op_date,
    f.flight_number,
    pf.airline_code,
    COALESCE(pf.etd_change_count, 0)    AS etd_change_count,
    COALESCE(pf.total_etd_shift_min, 0) AS total_etd_shift_min,
    LEAST(
        100,
        100
        - 10 * COALESCE(pf.etd_change_count, 0)
        - 0.5 * GREATEST(COALESCE(pf.total_etd_shift_min, 0), 0)
    ) AS stability_score
FROM per_flight pf
JOIN dxb_ops.flights_live f USING (flight_id);

-- =========================================
-- G. RINGKASAN & DETAIL
-- =========================================

-- 1) Summary per airline (dipakai di Jupyter untuk bar chart)
SELECT
    airline_code,
    COUNT(*)                 AS flights,
    AVG(etd_change_count)    AS avg_etd_changes,
    AVG(total_etd_shift_min) AS avg_shift_min,
    AVG(stability_score)     AS avg_stability_score
FROM dxb_ops.fact_flight_stability
GROUP BY airline_code
ORDER BY avg_stability_score ASC;

-- 2) Detail per flight
SELECT *
FROM dxb_ops.fact_flight_stability
ORDER BY stability_score ASC;
