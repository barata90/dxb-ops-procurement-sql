-- DNATA DXB TRAINING – CLEAN SCRIPT (FIXED FOR PSQL/JUPYTER)

-- 0. Cleanup
DROP TABLE IF EXISTS task_assignments;
DROP TABLE IF EXISTS flight_task_requirements;
DROP TABLE IF EXISTS rosters;
DROP TABLE IF EXISTS baggage_incidents;
DROP TABLE IF EXISTS baggage_scans;
DROP TABLE IF EXISTS baggage_flights;
DROP TABLE IF EXISTS turnaround_events;
DROP TABLE IF EXISTS flights;
DROP TABLE IF EXISTS airlines;
DROP TABLE IF EXISTS terminals;

-- 1. Dimensions
CREATE TABLE airlines (
    airline_code   VARCHAR(5) PRIMARY KEY,
    airline_name   VARCHAR(100),
    hub_airport    VARCHAR(10),
    alliance       VARCHAR(20)
);

INSERT INTO airlines (airline_code, airline_name, hub_airport, alliance) 
VALUES
('EK', 'Emirates',           'DXB', 'none'),
('QF', 'Qantas',             'SYD', 'oneworld'),
('BA', 'British Airways',    'LHR', 'oneworld'),
('QR', 'Qatar Airways',      'DOH', 'oneworld'),
('SQ', 'Singapore Airlines', 'SIN', 'star'),
('AF', 'Air France',         'CDG', 'skyteam');

CREATE TABLE terminals (
    terminal_code   VARCHAR(5) PRIMARY KEY,
    description     VARCHAR(100)
);

INSERT INTO terminals (terminal_code, description) VALUES
('T1', 'Dubai International – Terminal 1'),
('T2', 'Dubai International – Terminal 2'),
('T3', 'Dubai International – Emirates Terminal 3');

-- 2. Flights (arr/dep at DXB)
CREATE TABLE flights (
    flight_id        SERIAL PRIMARY KEY,
    flight_number    VARCHAR(10),
    airline_code     VARCHAR(5) REFERENCES airlines(airline_code),
    origin           VARCHAR(10),
    destination      VARCHAR(10),
    sta              TIMESTAMP,
    ata              TIMESTAMP,
    std              TIMESTAMP,
    atd              TIMESTAMP,
    terminal_code    VARCHAR(5) REFERENCES terminals(terminal_code),
    stand            VARCHAR(10),
    is_turnaround    BOOLEAN
);

INSERT INTO flights
(flight_number, airline_code, origin, destination,
 sta, ata, std, atd, terminal_code, stand, is_turnaround)
VALUES
('EK202', 'EK', 'JFK', 'DXB',
 '2025-11-10 05:35'::timestamp, '2025-11-10 05:45'::timestamp,
 '2025-11-10 07:45'::timestamp, '2025-11-10 07:55'::timestamp,
 'T3', 'A10', TRUE),

('EK016', 'EK', 'LHR', 'DXB',
 '2025-11-10 06:05'::timestamp, '2025-11-10 06:02'::timestamp,
 '2025-11-10 08:20'::timestamp, '2025-11-10 08:18'::timestamp,
 'T3', 'A12', TRUE),

('EK432', 'EK', 'DXB', 'BNE',
 '2025-11-10 08:45'::timestamp, '2025-11-10 08:50'::timestamp,
 '2025-11-10 08:45'::timestamp, '2025-11-10 08:50'::timestamp,
 'T3', 'A16', FALSE),

('BA108', 'BA', 'DXB', 'LHR',
 '2025-11-10 09:00'::timestamp, '2025-11-10 09:10'::timestamp,
 '2025-11-10 10:10'::timestamp, '2025-11-10 10:40'::timestamp,
 'T1', 'B22', TRUE),

('QR1019', 'QR', 'DOH', 'DXB',
 '2025-11-10 07:40'::timestamp, '2025-11-10 07:55'::timestamp,
 '2025-11-10 09:10'::timestamp, '2025-11-10 09:25'::timestamp,
 'T1', 'C05', TRUE),

('SQ495', 'SQ', 'DXB', 'SIN',
 '2025-11-10 08:15'::timestamp, '2025-11-10 08:18'::timestamp,
 '2025-11-10 09:30'::timestamp, '2025-11-10 09:55'::timestamp,
 'T1', 'B24', TRUE);

-- 3. Turnaround events
CREATE TABLE turnaround_events (
    event_id     SERIAL PRIMARY KEY,
    flight_id    INT REFERENCES flights(flight_id),
    event_type   VARCHAR(50),
    event_time   TIMESTAMP,
    workgroup    VARCHAR(30),
    source       VARCHAR(30)
);

INSERT INTO turnaround_events (flight_id, event_type, event_time, 
                               workgroup, source)
SELECT f.flight_id, e.event_type, e.event_time, e.workgroup, e.source
FROM (
    VALUES
      ('EK202', 'DOCK_ON',           '2025-11-10 05:47'::timestamp, 'RAMP',    'RMS'),
      ('EK202', 'DOORS_OPEN',        '2025-11-10 05:49'::timestamp, 'RAMP',    'RMS'),
      ('EK202', 'FIRST_BAG_OFF',     '2025-11-10 05:55'::timestamp, 'BAGGAGE', 'BRS'),
      ('EK202', 'LAST_BAG_OFF',      '2025-11-10 06:18'::timestamp, 'BAGGAGE', 'BRS'),
      ('EK202', 'CLEANING_ON',       '2025-11-10 06:00'::timestamp, 'CLEANING','RMS'),
      ('EK202', 'CLEANING_COMPLETE', '2025-11-10 06:35'::timestamp, 'CLEANING','RMS'),
      ('EK202', 'CATERING_ON',       '2025-11-10 06:20'::timestamp, 'CATERING','RMS'),
      ('EK202', 'BOARDING_START',    '2025-11-10 07:10'::timestamp, 'RAMP',    'RMS'),
      ('EK202', 'BOARDING_COMPLETE', '2025-11-10 07:40'::timestamp, 'RAMP',    'RMS'),
      ('EK202', 'DOORS_CLOSED',      '2025-11-10 07:43'::timestamp, 'RAMP',    'RMS'),

      ('EK016', 'DOCK_ON',           '2025-11-10 06:04'::timestamp, 'RAMP',    'RMS'),
      ('EK016', 'DOORS_OPEN',        '2025-11-10 06:05'::timestamp, 'RAMP',    'RMS'),
      ('EK016', 'FIRST_BAG_OFF',     '2025-11-10 06:12'::timestamp, 'BAGGAGE', 'BRS'),
      ('EK016', 'LAST_BAG_OFF',      '2025-11-10 06:32'::timestamp, 'BAGGAGE', 'BRS'),
      ('EK016', 'CATERING_ON',       '2025-11-10 06:25'::timestamp, 'CATERING','RMS'),
      ('EK016', 'BOARDING_START',    '2025-11-10 07:45'::timestamp, 'RAMP',    'RMS'),
      ('EK016', 'BOARDING_COMPLETE', '2025-11-10 08:10'::timestamp, 'RAMP',    'RMS'),
      ('EK016', 'DOORS_CLOSED',      '2025-11-10 08:15'::timestamp, 'RAMP',    'RMS'),

      ('BA108', 'DOCK_ON',           '2025-11-10 09:12'::timestamp, 'RAMP',    'RMS'),
      ('BA108', 'DOORS_OPEN',        '2025-11-10 09:12'::timestamp, 'RAMP',    'RMS'),
      ('BA108', 'FIRST_BAG_OFF',     '2025-11-10 09:20'::timestamp, 'BAGGAGE', 'BRS'),
      ('BA108', 'LAST_BAG_OFF',      '2025-11-10 09:48'::timestamp, 'BAGGAGE', 'BRS'),
      ('BA108', 'CLEANING_ON',       '2025-11-10 09:25'::timestamp, 'CLEANING','RMS'),
      ('BA108', 'CLEANING_COMPLETE', '2025-11-10 09:55'::timestamp, 'CLEANING','RMS'),
      ('BA108', 'BOARDING_START',    '2025-11-10 10:05'::timestamp, 'RAMP',    'RMS'),
      ('BA108', 'BOARDING_COMPLETE', '2025-11-10 10:30'::timestamp, 'RAMP',    'RMS'),
      ('BA108', 'DOORS_CLOSED',      '2025-11-10 10:35'::timestamp, 'RAMP',    'RMS')
) AS e(flight_number, event_type, event_time, workgroup, source)
JOIN flights f ON f.flight_number = e.flight_number;

-- 4. Baggage tables
CREATE TABLE baggage_flights (
    bflight_id     SERIAL PRIMARY KEY,
    flight_id      INT REFERENCES flights(flight_id),
    total_bags     INT,
    transfer_bags  INT,
    local_bags     INT
);

INSERT INTO baggage_flights (flight_id, total_bags, transfer_bags, local_bags)
SELECT f.flight_id, x.total_bags, x.transfer_bags, x.local_bags
FROM (
    VALUES
      ('EK202', 260, 180, 80),
      ('EK016', 210, 150, 60),
      ('BA108', 180, 60, 120),
      ('QR1019', 90,  40, 50),
      ('SQ495', 140, 70, 70)
) AS x(flight_number, total_bags, transfer_bags, local_bags)
JOIN flights f ON f.flight_number = x.flight_number;

CREATE TABLE baggage_scans (
    bag_tag     VARCHAR(20),
    flight_id   INT REFERENCES flights(flight_id),
    scan_type   VARCHAR(20),
    scan_time   TIMESTAMP,
    location    VARCHAR(50),
    is_transfer BOOLEAN
);

INSERT INTO baggage_scans (bag_tag, flight_id, scan_type, scan_time, location, is_transfer)
SELECT
    s.bag_tag,
    f.flight_id,
    s.scan_type,
    s.scan_time,
    s.location,
    s.is_transfer
FROM (
    VALUES
    ('EK202T0001', 'EK202',  'OFFLOAD',      '2025-11-10 05:56'::timestamp, 'BHS_T3_IN_01',  TRUE),
    ('EK202T0001', 'EK202',  'ONLOAD_TRANS', '2025-11-10 07:05'::timestamp, 'MAKEUP_T3_Z3',  TRUE),
    ('EK202T0002', 'EK202',  'OFFLOAD',      '2025-11-10 06:00'::timestamp, 'BHS_T3_IN_01',  TRUE),
    ('EK202T0002', 'EK202',  'ONLOAD_TRANS', '2025-11-10 07:10'::timestamp, 'MAKEUP_T3_Z3',  TRUE),
    ('EK202L0001', 'EK202',  'OFFLOAD',      '2025-11-10 05:58'::timestamp, 'BHS_T3_IN_01',  FALSE),
    ('BA108L0001', 'BA108',  'ONLOAD_LOCAL', '2025-11-10 09:35'::timestamp, 'CHECKIN_T1_B',  FALSE),
    ('BA108L0002', 'BA108',  'ONLOAD_LOCAL', '2025-11-10 09:42'::timestamp, 'CHECKIN_T1_B',  FALSE),
    ('QR1019T0001','QR1019', 'OFFLOAD',      '2025-11-10 08:00'::timestamp, 'BHS_T1_IN_03',  TRUE),
    ('QR1019T0001','QR1019', 'ONLOAD_TRANS', '2025-11-10 08:40'::timestamp, 'MAKEUP_T1_Z1',  TRUE)
) AS s(bag_tag, flight_number, scan_type, scan_time, location, is_transfer)
JOIN flights f ON f.flight_number = s.flight_number;

CREATE TABLE baggage_incidents (
    incident_id   SERIAL PRIMARY KEY,
    flight_id     INT REFERENCES flights(flight_id),
    bag_tag       VARCHAR(20),
    incident_type VARCHAR(20),
    status        VARCHAR(20),
    cause_code    VARCHAR(20),
    created_at    TIMESTAMP,
    closed_at     TIMESTAMP
);

INSERT INTO baggage_incidents
(flight_id, bag_tag, incident_type, status, cause_code, created_at, closed_at)
SELECT
    f.flight_id,
    x.bag_tag,
    x.incident_type,
    x.status,
    x.cause_code,
    x.created_at,
    x.closed_at
FROM (
    VALUES
      ('EK202', 'EK202T0002', 'DELAYED', 'RESOLVED', 'SHORT_CONNECTION',
       '2025-11-10 09:30'::timestamp, '2025-11-10 13:00'::timestamp),
      ('BA108', 'BA108L0002', 'MISSING', 'OPEN',     'SORT_ERROR',
       '2025-11-10 10:50'::timestamp, NULL)
) AS x(flight_number, bag_tag, incident_type, status, cause_code, created_at, closed_at)
JOIN flights f ON f.flight_number = x.flight_number;

-- 5. Workforce / manpower
CREATE TABLE rosters (
    roster_id   SERIAL PRIMARY KEY,
    staff_id    INT,
    staff_name  VARCHAR(100),
    role        VARCHAR(30),
    terminal    VARCHAR(5),
    shift_start TIMESTAMP,
    shift_end   TIMESTAMP
);

INSERT INTO rosters (staff_id, staff_name, role, terminal, shift_start, shift_end) VALUES
(9001, 'Ali Ramp',     'RAMP_AGENT',    'T3', '2025-11-10 05:30'::timestamp, '2025-11-10 13:30'::timestamp),
(9002, 'Hassan Ramp',  'RAMP_AGENT',    'T3', '2025-11-10 06:00'::timestamp, '2025-11-10 14:00'::timestamp),
(9003, 'Sara Bags',    'BAGGAGE_AGENT', 'T3', '2025-11-10 05:30'::timestamp, '2025-11-10 13:30'::timestamp),
(9004, 'John Bags',    'BAGGAGE_AGENT', 'T3', '2025-11-10 06:00'::timestamp, '2025-11-10 14:00'::timestamp),
(9005, 'Mariam Sup',   'SUPERVISOR',    'T3', '2025-11-10 05:30'::timestamp, '2025-11-10 13:30'::timestamp),
(9010, 'Omar Ramp T1', 'RAMP_AGENT',    'T1', '2025-11-10 07:30'::timestamp, '2025-11-10 15:30'::timestamp);

CREATE TABLE flight_task_requirements (
    req_id         SERIAL PRIMARY KEY,
    flight_id      INT REFERENCES flights(flight_id),
    role           VARCHAR(30),
    location       VARCHAR(50),
    start_time     TIMESTAMP,
    end_time       TIMESTAMP,
    required_staff INT
);

INSERT INTO flight_task_requirements
(flight_id, role, location, start_time, end_time, required_staff)
SELECT
    f.flight_id,
    x.role,
    x.location,
    x.start_time,
    x.end_time,
    x.required_staff
FROM (
    VALUES
      ('EK202', 'RAMP_AGENT',    'Stand A10',        '2025-11-10 05:40'::timestamp, '2025-11-10 08:00'::timestamp, 5),
      ('EK202', 'BAGGAGE_AGENT', 'T3 Inbound Belts', '2025-11-10 05:50'::timestamp, '2025-11-10 06:40'::timestamp, 4),
      ('EK016', 'RAMP_AGENT',    'Stand A12',        '2025-11-10 06:00'::timestamp, '2025-11-10 08:30'::timestamp, 4),
      ('EK016', 'BAGGAGE_AGENT', 'T3 Inbound Belts', '2025-11-10 06:10'::timestamp, '2025-11-10 06:50'::timestamp, 3),
      ('BA108', 'RAMP_AGENT',    'Stand B22',        '2025-11-10 09:05'::timestamp, '2025-11-10 10:45'::timestamp, 4),
      ('BA108', 'BAGGAGE_AGENT', 'T1 Inbound Belts', '2025-11-10 09:15'::timestamp, '2025-11-10 09:55'::timestamp, 3)
) AS x(flight_number, role, location, start_time, end_time, required_staff)
JOIN flights f ON f.flight_number = x.flight_number;

CREATE TABLE task_assignments (
    assign_id      SERIAL PRIMARY KEY,
    flight_id      INT REFERENCES flights(flight_id),
    staff_id       INT,
    role           VARCHAR(30),
    assigned_start TIMESTAMP,
    assigned_end   TIMESTAMP
);

INSERT INTO task_assignments (flight_id, staff_id, role, assigned_start, assigned_end)
SELECT
    f.flight_id,
    x.staff_id,
    x.role,
    x.assigned_start,
    x.assigned_end
FROM (
    VALUES
      ('EK202', 9001, 'RAMP_AGENT',    '2025-11-10 05:40'::timestamp, '2025-11-10 07:50'::timestamp),
      ('EK202', 9002, 'RAMP_AGENT',    '2025-11-10 05:45'::timestamp, '2025-11-10 07:45'::timestamp),
      ('EK202', 9003, 'BAGGAGE_AGENT', '2025-11-10 05:50'::timestamp, '2025-11-10 06:40'::timestamp),
      ('EK016', 9001, 'RAMP_AGENT',    '2025-11-10 06:05'::timestamp, '2025-11-10 08:10'::timestamp),
      ('EK016', 9004, 'BAGGAGE_AGENT', '2025-11-10 06:15'::timestamp, '2025-11-10 06:55'::timestamp),
      ('BA108', 9010, 'RAMP_AGENT',    '2025-11-10 09:10'::timestamp, '2025-11-10 10:40'::timestamp)
) AS x(flight_number, staff_id, role, assigned_start, assigned_end)
JOIN flights f ON f.flight_number = x.flight_number;
