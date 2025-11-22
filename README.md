# DXB Operations & Engineering Procurement – SQL Lab

Author: [@barata90](https://github.com/barata90)  
Tech stack: PostgreSQL, DBeaver, Advanced SQL (CTEs, window functions, triggers)

This project is a mini data mart built in PostgreSQL to simulate **Dubai International Airport (DXB)** operations 
and **engineering procurement analytics**.

It is designed as a **portfolio-style project** to demonstrate:

- solid SQL modelling and schema design,  
- advanced querying (window functions, CTEs, analytic aggregates),  
- trigger-based history (SCD2-style) for operational data, and  
- how to translate data into concrete business insights for **airport operations** and **procurement**.

---

## 1. Project Overview

The database is split into three main schemas:

1. `public` – **Airport operations**
   - Flight schedule & status
   - Turnaround events (ground handling timeline)
   - Baggage scans & incidents
   - Workforce rosters and task requirements

2. `dxb_ops` – **Operations analytics**
   - Temporal tables for **flight schedule stability**
   - Fact table with **stability score** per flight/day

3. `eng_proc` – **Engineering procurement**
   - Aircraft parts, suppliers, and contracts
   - Purchase orders & line items
   - Part failures / AOG interruptions
   - Part usage and Life-Cycle Cost (LCC) scenarios

The goal is to mimic realistic problems an analyst or data engineer would face in an airline / MRO context.

---

## 2. Data Model

### 2.1 `public` schema – Operational layer

Key tables (simplified):

- `flights`
  - One row per movement at DXB  
  - Columns: `flight_number`, `airline_code`, `origin`, `destination`,  scheduled/estimated/actual departure & arrival
    (`std`, `sta`, `etd`, `eta`, `atd`, `ata`), `stand`, `terminal_code`.

- `turnaround_events`
  - Milestones for ground handling (e.g. `DOCK_ON`, `BOARDING_START`, `CLEANING_COMPLETE`).
  - Columns: `flight_id`, `event_type`, `event_time`, `workcentre`.

- `baggage_scans`
  - Scan events for bags (`OFFLOAD`, `ONLOAD_TRANS`, etc.).
  - Used to approximate baggage dwell times and mis-handled risk.

- `baggage_incidents`
  - Lost / delayed / damaged baggage records.

- `rosters`
  - Staff rosters with `staff_id`, `role` (e.g. `RAMP_AGENT`, `BAGGAGE_AGENT`), `terminal`, `shift_start`, `shift_end`.

- `flight_task_requirements`
  - Required manpower per flight, per role, per time window.

Together these tables represent a simplified but realistic DXB hub-operation model.

---

### 2.2 `dxb_ops` schema – Flight stability analytics

- `flights_live`
  - Current state of each flight at DXB.
  - Contains operational date, airline, origin/destination, all schedule/estimate/actual times
    and current `status_code` (e.g. `SCHED`, `BOARDING`, `DEPARTED`).

- `flights_history`
  - **SCD2-style history** of `flights_live`.
  - Every insert/update on `flights_live` writes a snapshot row with `valid_from`, `valid_to`, `change_reason`, `changed_by`.

- `fact_flight_stability`
  - Aggregated snapshot per flight/day containing:
    - `etd_change_count` – how many times ETD changed
    - `total_etd_shift_min` – net shift of ETD in minutes
    - `stability_score` – 0–100 score (higher = more stable)

#### Trigger

A PL/pgSQL trigger `dxb_ops.flights_live_audit`:

- On **INSERT** → creates the initial history row.  
- On **UPDATE** → closes the previous history row (`valid_to = now()`) and inserts a new one.  
- Optionally labels `change_reason` as:
  - `ETD_CHANGE`, `STATUS_CHANGE`, `GATE_CHANGE`, or `OTHER_UPDATE`.

This simulates an operational system where ATC / OCC continuously updates flight times and statuses.

---

### 2.3 `eng_proc` schema – Engineering procurement & LCC

Key tables:

- `parts`
  - Aircraft parts, with ATA chapter, family, and repairable flag.

- `suppliers`
  - Suppliers / OEMs, with simple quality and on-time ratings.

- `contracts` and `contract_prices`
  - Contract header (supplier, start/end date, discount, warranty months).
  - Price evolution per part, effective dates, and escalation assumptions.

- `po_headers`, `po_lines`
  - Purchase orders for parts, with quantities and delivered dates.

- `interruptions`
  - Unplanned events (e.g. AOG) linked to flights and root causes.

- `part_usage_monthly`
  - Aggregated usage statistics: flight hours, cycles, removals per part per month.

- `lcc_scenarios`
  - Parameters to run **Life-Cycle Cost** scenarios per part & supplier option:
    - `fh_per_failure` (MTBUR)
    - Base `unit_price`
    - `annual_esc_pct` (price escalation)
    - `interrupt_cost_per_event`
    - `discount_rate` and horizon in years

This schema allows realistic engineering & procurement analysis such as 
**reliability**, **supplier performance**, and **total cost of ownership**.

---

## 3. Analytics Use Cases

### 3.1 Flight schedule stability (DXB operations)

**Business question:**  
> Which flights and airlines have the most unstable ETD (many changes / big shifts)?

**Approach:**

1. Use `flights_history` as a temporal table.
2. Use window functions (e.g. `LAG`) by `flight_id` ordered by `valid_from` to detect each ETD change.
3. For each flight:
   - Count ETD changes → `etd_change_count`
   - Compute `total_etd_shift_min` = difference between earliest and latest ETD.
4. Calculate a simple **stability_score**:
   - Start at 100
   - Subtract points per change and per minute of total shift.

Example output (per flight):

| op_date    | flight_number | airline_code | etd_change_count | total_etd_shift_min | stability_score |
|----------- |---------------|--------------|------------------|---------------------|-----------------|
| 2025-11-10 | BA108         | BA           | 2                | 30                  | 65              |
| 2025-11-10 | EK202         | EK           | 0                | 0                   | 100             |

Example aggregation (per airline):

| airline_code | flights | avg_etd_changes | avg_shift_min | avg_stability_score |
|--------------|---------|-----------------|---------------|---------------------|
| BA           | 1       | 2.0             | 30.0          | 65.0                |
| EK, QR, SQ   | …       | 0.0             | 0.0           | 100.0               |

This can be used as a **flight or airline scorecard** in operational performance reviews.

---

### 3.2 Manpower vs demand (ground handling)

**Business question:**  
> During the morning wave at DXB, where are we under-staffed by role and hour?

**Approach:**

1. From `flight_task_requirements`, aggregate **required staff** per `ops_date`, `hour_slot`, `role`.
2. From `rosters`, aggregate **available staff** per `shift_date`, `hour_slot`, `role`.
3. Left join demand vs supply to compute:
   - `staffing_gap = available_staff - total_required_staff`
   - Categorise each hour & role into:
     - `CRITICAL` (gap ≤ -2)
     - `UNDER`
     - `BALANCED`
     - `SURPLUS`

Example output:

| ops_date   | hour_slot           | role           | total_required_staff | available_staff | staffing_gap |
|----------- |---------------------|----------------|----------------------|-----------------|--------------|
| 2025-11-10 | 05:00–06:00         | BAGGAGE_AGENT  | 4                    | 1               | -3           |
| 2025-11-10 | 05:00–06:00         | RAMP_AGENT     | 5                    | 1               | -4           |
| 2025-11-10 | 09:00–10:00         | BAGGAGE_AGENT  | 3                    | 0               | -3           |

This directly pinpoints **when and where** management should add staff or adjust shift patterns.

---

### 3.3 Procurement Life-Cycle Cost (LCC) for aircraft parts

**Business question:**  
> For a safety-critical part (e.g. A380 brake), which supplier option is cheaper over a 10-year horizon?

**Approach:**

1. Use `part_usage_monthly` and `interruptions` to estimate:
   - `fh_per_failure` (Mean Time Between Unscheduled Removal).
   - Annual number of interruption events.

2. Configure scenarios in `lcc_scenarios`:
   - OEM baseline vs aggressive MRO package.
   - Different unit prices, escalation rates, and reliability.

3. For each year in the horizon:
   - Compute expected failures and interruptions.
   - Estimate:
     - Replacement cost
     - Interruption cost (e.g. AOG)
   - Apply annual price escalation and discount factor.

4. Compute **NPV of total cost** per scenario in pure SQL (CTEs + window functions).

Example comparative result (approximate):

| scenario_name   | part_number | supplier_name    | npv_total_cost |
|---------------- |-------------|----------------  |----------------|
| OEM_A baseline  | BRAKE-A380  | OEM Aero Systems | 3.47M          |
| MRO_X aggressive| BRAKE-A380  | MRO X Dubai      | 3.94M          |

Although the MRO option may look cheaper on a per-event basis, once we 
factor in reliability and interruption costs, the OEM baseline is **~470k cheaper over the life-cycle**.

This is exactly the type of analysis a **Procurement Analyst – Engineering** role would perform.

---
To open the full SQL lab:
- Download General-20251122.dbp (click the file, then Download raw).
- Open DBeaver → File → Import → DBeaver → Project.
- Select the .dbp file and import.
This will load all connections, ER diagrams, and SQL scripts used in the lab.

---

## 4. Repository Structure

```text
├── sql/
│   ├── 01_public_dxb_ops.sql          # flights, baggage, rosters, etc.
│   ├── 02_dxb_ops_flight_stability.sql # flights_live/history + fact table
│   └── 03_eng_proc_lcc.sql            # procurement & life-cycle cost schema
├── docs/
│   ├── schema_public.png              # ER diagram screenshots 
│   ├── schema_dxb_ops.png
│   ├── schema_eng_proc.png
│   └── sample_queries.png
├── dbeaver/
│   └── General-20251122.dbp           # exported DBeaver project
└── README.md

