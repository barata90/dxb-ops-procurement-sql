CREATE SCHEMA IF NOT EXISTS eng_proc;

-- 1) Supplier master
CREATE TABLE eng_proc.suppliers (
    supplier_id      SERIAL PRIMARY KEY,
    supplier_code    VARCHAR(20) UNIQUE NOT NULL,
    supplier_name    VARCHAR(100) NOT NULL,
    region           VARCHAR(20),          -- EMEA / APAC / etc
    oem_flag         BOOLEAN,              -- true kalau OEM, false kalau 
MRO/independent
    quality_rating   NUMERIC(3,1),         -- 1-5 (internal score)
    on_time_rating   NUMERIC(3,1)          -- 1-5
);

-- 2) Part master (technical)
CREATE TABLE eng_proc.parts (
    part_id          SERIAL PRIMARY KEY,
    part_number      VARCHAR(40) UNIQUE NOT NULL,
    description      VARCHAR(200),
    ata_chapter      VARCHAR(10),          -- technical ATA reference
    aircraft_family  VARCHAR(20),          -- A380, B777, A320...
    repairable_flag  BOOLEAN,              -- rotable vs consumable
    uom              VARCHAR(10)           -- EA, SET, L, KG...
);

-- 3) Contracts with suppliers
CREATE TABLE eng_proc.contracts (
    contract_id      SERIAL PRIMARY KEY,
    supplier_id      INT REFERENCES eng_proc.suppliers(supplier_id),
    contract_code    VARCHAR(30) UNIQUE NOT NULL,
    start_date       DATE NOT NULL,
    end_date         DATE,
    currency         VARCHAR(3) NOT NULL,  -- USD, EUR, AED
    discount_pct     NUMERIC(5,2),         -- vs list price
    warranty_months  INT,
    pbth_flag        BOOLEAN DEFAULT FALSE, -- Power-By-The-Hour?
    remarks          TEXT
);

-- 4) Contract prices per part (time-dependent)
CREATE TABLE eng_proc.contract_prices (
    contract_price_id SERIAL PRIMARY KEY,
    contract_id       INT REFERENCES eng_proc.contracts(contract_id),
    part_id           INT REFERENCES eng_proc.parts(part_id),
    effective_from    DATE NOT NULL,
    effective_to      DATE,
    unit_price        NUMERIC(14,4),
    currency          VARCHAR(3) NOT NULL
);

-- 5) Purchase orders & lines
CREATE TABLE eng_proc.po_headers (
    po_id            SERIAL PRIMARY KEY,
    po_number        VARCHAR(30) UNIQUE NOT NULL,
    supplier_id      INT REFERENCES eng_proc.suppliers(supplier_id),
    po_date          DATE NOT NULL,
    contract_id      INT REFERENCES eng_proc.contracts(contract_id),
    status           VARCHAR(20),          -- OPEN / CLOSED / CANCELLED
    payment_terms    VARCHAR(50)
);

CREATE TABLE eng_proc.po_lines (
    po_line_id       SERIAL PRIMARY KEY,
    po_id            INT REFERENCES eng_proc.po_headers(po_id),
    line_no          INT NOT NULL,
    part_id          INT REFERENCES eng_proc.parts(part_id),
    quantity         NUMERIC(12,2) NOT NULL,
    unit_price       NUMERIC(14,4) NOT NULL,
    currency         VARCHAR(3) NOT NULL,
    promised_date    DATE,
    delivered_date   DATE
);

-- 6) Reliability / usage (link ke engineering)
CREATE TABLE eng_proc.part_usage_monthly (
    part_id          INT REFERENCES eng_proc.parts(part_id),
    aircraft_family  VARCHAR(20),
    year_month       DATE,                 -- 1st of month
    flight_hours     NUMERIC(12,1),
    flight_cycles    NUMERIC(12,1),
    removals         INT,                  -- unscheduled
    CONSTRAINT pk_usage PRIMARY KEY (part_id, year_month)
);

-- 7) Operational interruptions attributable to parts
CREATE TABLE eng_proc.interruptions (
    interrupt_id     SERIAL PRIMARY KEY,
    event_date       DATE,
    flight_number    VARCHAR(10),
    delay_minutes    INT,
    cancellation     BOOLEAN,
    part_id          INT REFERENCES eng_proc.parts(part_id),
    root_cause       VARCHAR(50)           -- TECH / LOG / SUPP CHAIN etc
);


WITH usage AS (
    SELECT
        u.part_id,
        SUM(u.flight_hours)  AS fh,
        SUM(u.removals)      AS removals
    FROM eng_proc.part_usage_monthly u
    GROUP BY u.part_id
),
fail_rate AS (
    SELECT
        part_id,
        fh,
        removals,
        CASE WHEN removals = 0 THEN NULL
             ELSE fh::NUMERIC / removals
        END AS fh_per_failure  -- MTBUR approx
    FROM usage
),
avg_price AS (
    SELECT
        cp.part_id,
        c.supplier_id,
        AVG(cp.unit_price) AS avg_price
    FROM eng_proc.contract_prices cp
    JOIN eng_proc.contracts c ON c.contract_id = cp.contract_id
    GROUP BY cp.part_id, c.supplier_id
)
SELECT
    p.part_number,
    s.supplier_name,
    f.fh_per_failure,
    a.avg_price,
    -- assume setiap failure perlu 1 unit
    a.avg_price / f.fh_per_failure AS dmc_per_fh
FROM fail_rate f
JOIN avg_price a USING (part_id)
JOIN eng_proc.parts p USING (part_id)
JOIN eng_proc.suppliers s ON s.supplier_id = a.supplier_id
WHERE f.fh_per_failure IS NOT NULL;

CREATE TABLE eng_proc.lcc_scenarios (
    scenario_id      SERIAL PRIMARY KEY,
    scenario_name    VARCHAR(50),
    part_id          INT REFERENCES eng_proc.parts(part_id),
    supplier_id      INT REFERENCES eng_proc.suppliers(supplier_id),
    horizon_years    INT,
    discount_rate    NUMERIC(5,4),    -- misal 0.08 = 8% / year
    annual_fh        NUMERIC(10,1),   -- flight hours per year
    fh_per_failure   NUMERIC(10,1),   -- expected MTBUR
    base_unit_price  NUMERIC(14,4),
    annual_esc_pct   NUMERIC(5,2),    -- price escalation %
    interrupt_cost_per_event NUMERIC(14,4)  -- cost of one AOG/interrupt
);

WITH cashflows AS (
    SELECT
        s.scenario_id,
        year,
        -- expected failures per year = annual_fh / fh_per_failure
        (s.annual_fh / s.fh_per_failure) AS failures_per_year,
        -- unit price per year with escalation
        s.base_unit_price * POWER(1 + s.annual_esc_pct/100.0, year-1) AS 
unit_price_year,
        -- material cost per year
        (s.annual_fh / s.fh_per_failure) 
            * s.base_unit_price * POWER(1 + s.annual_esc_pct/100.0, 
year-1) AS material_cost,
        -- interruption cost (misal setiap failure → 1 event)
        (s.annual_fh / s.fh_per_failure) * s.interrupt_cost_per_event AS 
interrupt_cost,
        -- total nominal cash out
        (s.annual_fh / s.fh_per_failure) 
            * s.base_unit_price * POWER(1 + s.annual_esc_pct/100.0, 
year-1)
          + (s.annual_fh / s.fh_per_failure) * s.interrupt_cost_per_event 
AS total_cost
    FROM eng_proc.lcc_scenarios s
    CROSS JOIN LATERAL generate_series(1, s.horizon_years) AS year
),
npv AS (
    SELECT
        c.scenario_id,
        SUM(c.total_cost / POWER(1 + s.discount_rate, c.year)) AS 
npv_total_cost
    FROM cashflows c
    JOIN eng_proc.lcc_scenarios s USING (scenario_id)
    GROUP BY c.scenario_id
)
SELECT
    s.scenario_name,
    p.part_number,
    sup.supplier_name,
    n.npv_total_cost
FROM npv n
JOIN eng_proc.lcc_scenarios s USING (scenario_id)
JOIN eng_proc.parts p ON p.part_id = s.part_id
JOIN eng_proc.suppliers sup ON sup.supplier_id = s.supplier_id
ORDER BY n.npv_total_cost;


SELECT * FROM eng_proc.lcc_scenarios;

INSERT INTO eng_proc.suppliers (
    supplier_code, supplier_name, region, oem_flag, quality_rating, 
on_time_rating
) VALUES
    ('OEM_A',  'OEM Aero Systems',      'EU',  TRUE,  4.5, 4.2),
    ('MRO_X',  'MRO X Dubai',           'MEA', FALSE, 4.0, 3.8)
ON CONFLICT (supplier_code) DO NOTHING;


INSERT INTO eng_proc.parts (
    part_number, description, ata_chapter, aircraft_family, 
repairable_flag, uom
) VALUES
    ('BRAKE-A380', 'A380 Brake Unit', '32', 'A380', TRUE, 'EA')
ON CONFLICT (part_number) DO NOTHING;


-- Skenario 1: OEM_A – harga mahal, reliabilitas bagus
INSERT INTO eng_proc.lcc_scenarios (
    scenario_name,
    part_id,
    supplier_id,
    horizon_years,
    discount_rate,
    annual_fh,
    fh_per_failure,
    base_unit_price,
    annual_esc_pct,
    interrupt_cost_per_event
)
SELECT
    'OEM_A baseline',
    p.part_id,
    s.supplier_id,
    10,          -- horizon 10 tahun
    0.08,        -- discount rate 8%
    20000,       -- 20.000 flight hours per tahun
    6000,        -- 6.000 FH per failure (lebih andal)
    120000,      -- harga unit awal USD 120k
    3,           -- eskalasi harga 3% / tahun
    20000        -- tiap interrupt biaya 20k
FROM eng_proc.parts p, eng_proc.suppliers s
WHERE p.part_number = 'BRAKE-A380'
  AND s.supplier_code = 'OEM_A';

-- Skenario 2: MRO_X – harga lebih murah, reliabilitas lebih jelek
INSERT INTO eng_proc.lcc_scenarios (
    scenario_name,
    part_id,
    supplier_id,
    horizon_years,
    discount_rate,
    annual_fh,
    fh_per_failure,
    base_unit_price,
    annual_esc_pct,
    interrupt_cost_per_event
)
SELECT
    'MRO_X aggressive',
    p.part_id,
    s.supplier_id,
    10,
    0.08,
    20000,
    4000,        -- lebih sering failure
    90000,       -- harga unit lebih murah
    2,           -- eskalasi 2%
    20000
FROM eng_proc.parts p, eng_proc.suppliers s
WHERE p.part_number = 'BRAKE-A380'
  AND s.supplier_code = 'MRO_X';

SELECT scenario_id, scenario_name
FROM eng_proc.lcc_scenarios;


WITH cashflows AS (
    SELECT
        s.scenario_id,
        year,
        (s.annual_fh / s.fh_per_failure) AS failures_per_year,
        s.base_unit_price * POWER(1 + s.annual_esc_pct/100.0, year-1) AS 
unit_price_year,
        (s.annual_fh / s.fh_per_failure) 
            * s.base_unit_price * POWER(1 + s.annual_esc_pct/100.0, 
year-1) AS material_cost,
        (s.annual_fh / s.fh_per_failure) * s.interrupt_cost_per_event AS 
interrupt_cost,
        (s.annual_fh / s.fh_per_failure) 
            * s.base_unit_price * POWER(1 + s.annual_esc_pct/100.0, 
year-1)
          + (s.annual_fh / s.fh_per_failure) * s.interrupt_cost_per_event 
AS total_cost
    FROM eng_proc.lcc_scenarios s
    CROSS JOIN LATERAL generate_series(1, s.horizon_years) AS year
),
npv AS (
    SELECT
        c.scenario_id,
        SUM(c.total_cost / POWER(1 + s.discount_rate, c.year)) AS 
npv_total_cost
    FROM cashflows c
    JOIN eng_proc.lcc_scenarios s USING (scenario_id)
    GROUP BY c.scenario_id
)
SELECT
    s.scenario_name,
    p.part_number,
    sup.supplier_name,
    n.npv_total_cost
FROM npv n
JOIN eng_proc.lcc_scenarios s USING (scenario_id)
JOIN eng_proc.parts p ON p.part_id = s.part_id
JOIN eng_proc.suppliers sup ON sup.supplier_id = s.supplier_id
ORDER BY n.npv_total_cost;


SELECT *
FROM eng_proc.lcc_scenarios
ORDER BY scenario_id;


WITH cashflows AS (
    SELECT
        s.scenario_name,
        year,
        (s.annual_fh / s.fh_per_failure) AS failures_per_year,
        s.base_unit_price * POWER(1 + s.annual_esc_pct/100.0, year-1) AS 
unit_price_year,
        (s.annual_fh / s.fh_per_failure) 
            * s.base_unit_price * POWER(1 + s.annual_esc_pct/100.0, 
year-1) AS material_cost,
        (s.annual_fh / s.fh_per_failure) * s.interrupt_cost_per_event AS 
interrupt_cost,
        (s.annual_fh / s.fh_per_failure) 
            * s.base_unit_price * POWER(1 + s.annual_esc_pct/100.0, 
year-1)
          + (s.annual_fh / s.fh_per_failure) * s.interrupt_cost_per_event 
AS total_cost
    FROM eng_proc.lcc_scenarios s
    CROSS JOIN LATERAL generate_series(1, s.horizon_years) AS year
)
SELECT *
FROM cashflows
WHERE scenario_name IN ('OEM_A baseline', 'MRO_X aggressive')
ORDER BY scenario_name, year;


-- Simpan hanya scenario_id terkecil untuk MRO_X, hapus yang lain
DELETE FROM eng_proc.lcc_scenarios
WHERE scenario_name = 'MRO_X aggressive'
  AND scenario_id <> (
      SELECT MIN(scenario_id)
      FROM eng_proc.lcc_scenarios
      WHERE scenario_name = 'MRO_X aggressive'
  );

SELECT scenario_id, scenario_name
FROM eng_proc.lcc_scenarios
ORDER BY scenario_id;


ALTER TABLE eng_proc.lcc_scenarios
ADD CONSTRAINT uq_lcc_scenario_name UNIQUE (scenario_name);


-- Average turnaround (ATA → ATD) per airline
SELECT
    airline_code,
    COUNT(*) AS flights,
    AVG(EXTRACT(EPOCH FROM (atd - ata))/60) AS avg_turnaround_min,
    MIN(EXTRACT(EPOCH FROM (atd - ata))/60) AS min_turnaround_min,
    MAX(EXTRACT(EPOCH FROM (atd - ata))/60) AS max_turnaround_min
FROM public.flights
WHERE ata IS NOT NULL
  AND atd IS NOT NULL
GROUP BY airline_code
ORDER BY avg_turnaround_min;


WITH demand AS (
    SELECT
        DATE(start_time) AS ops_date,
        DATE_TRUNC('hour', start_time) AS hour_slot,
        role,
        SUM(required_staff) AS total_required_staff
    FROM public.flight_task_requirements
    GROUP BY DATE(start_time), DATE_TRUNC('hour', start_time), role
),
supply AS (
    SELECT
        DATE(shift_start) AS ops_date,
        DATE_TRUNC('hour', shift_start) AS hour_slot,
        role,
        COUNT(DISTINCT staff_id) AS available_staff
    FROM public.rosters
    GROUP BY DATE(shift_start), DATE_TRUNC('hour', shift_start), role
)
SELECT
    d.ops_date,
    d.hour_slot,
    d.role,
    d.total_required_staff,
    COALESCE(s.available_staff, 0) AS available_staff,
    COALESCE(s.available_staff, 0) - d.total_required_staff AS 
staffing_gap
FROM demand d
LEFT JOIN supply s
  ON d.ops_date  = s.ops_date
 AND d.hour_slot = s.hour_slot
 AND d.role      = s.role
ORDER BY d.ops_date, d.hour_slot, d.role;



WITH cashflows AS (
    SELECT
        s.scenario_id,
        year,
        (s.annual_fh / s.fh_per_failure) AS failures_per_year,
        s.base_unit_price * POWER(1 + s.annual_esc_pct/100.0, year-1) AS 
unit_price_year,
        (s.annual_fh / s.fh_per_failure)
            * s.base_unit_price * POWER(1 + s.annual_esc_pct/100.0, 
year-1) AS material_cost,
        (s.annual_fh / s.fh_per_failure) * s.interrupt_cost_per_event AS 
interrupt_cost,
        (s.annual_fh / s.fh_per_failure)
            * s.base_unit_price * POWER(1 + s.annual_esc_pct/100.0, 
year-1)
          + (s.annual_fh / s.fh_per_failure) * s.interrupt_cost_per_event 
AS total_cost
    FROM eng_proc.lcc_scenarios s
    CROSS JOIN LATERAL generate_series(1, s.horizon_years) AS year
),
npv AS (
    SELECT
        c.scenario_id,
        SUM(c.total_cost / POWER(1 + s.discount_rate, c.year)) AS 
npv_total_cost
    FROM cashflows c
    JOIN eng_proc.lcc_scenarios s USING (scenario_id)
    GROUP BY c.scenario_id
)
SELECT
    s.scenario_name,
    p.part_number,
    sup.supplier_name,
    n.npv_total_cost
FROM npv n
JOIN eng_proc.lcc_scenarios s USING (scenario_id)
JOIN eng_proc.parts p ON p.part_id = s.part_id
JOIN eng_proc.suppliers sup ON sup.supplier_id = s.supplier_id
ORDER BY n.npv_total_cost;

