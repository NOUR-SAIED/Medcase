-- =============================================
-- HASH-BASED STAR SCHEMA ETL SCRIPT
-- =============================================

-- STEP 1: Create dimensions with hash columns
CREATE TABLE dim_diagnosis (
    diagnosis_key SERIAL PRIMARY KEY,
    diagnosis_group VARCHAR(255),
    severity_level VARCHAR(50),
    severity_confidence DECIMAL(5,4),
    organe_affecte VARCHAR(255),
    diagnosis_hash VARCHAR(64) UNIQUE NOT NULL
);

CREATE TABLE dim_patient (
    patient_key SERIAL PRIMARY KEY,
    age INT,
    sexe VARCHAR(10),
    patient_hash VARCHAR(64) UNIQUE NOT NULL
);

CREATE TABLE dim_department (
    department_key SERIAL PRIMARY KEY,
    department VARCHAR(255),
    department_hash VARCHAR(64) UNIQUE NOT NULL
);

CREATE TABLE dim_time (
    time_key SERIAL PRIMARY KEY,
    timestamp_date DATE UNIQUE,
    year INT,
    month INT,
    day INT
);

CREATE TABLE fact_medical_case (
    fact_key SERIAL PRIMARY KEY,
    diagnosis_key INT REFERENCES dim_diagnosis(diagnosis_key),
    patient_key INT REFERENCES dim_patient(patient_key),
    department_key INT REFERENCES dim_department(department_key),
    time_key INT REFERENCES dim_time(time_key),
    nb_cases INT DEFAULT 1
);

-- Indexes
CREATE INDEX idx_diagnosis_hash ON dim_diagnosis(diagnosis_hash);
CREATE INDEX idx_patient_hash ON dim_patient(patient_hash);
CREATE INDEX idx_department_hash ON dim_department(department_hash);
CREATE INDEX idx_time_date ON dim_time(timestamp_date);
CREATE INDEX idx_fact_diagnosis ON fact_medical_case(diagnosis_key);
CREATE INDEX idx_fact_patient ON fact_medical_case(patient_key);
CREATE INDEX idx_fact_department ON fact_medical_case(department_key);
CREATE INDEX idx_fact_time ON fact_medical_case(time_key);

-- STEP 2: Create staging table
CREATE TABLE staging_medical_data (
    NomFichier TEXT,
    object_id BIGINT,
    Description TEXT,
    Diagnosis TEXT,
    ClinicalPresentation TEXT,
    Commentary TEXT,
    Chapter TEXT,
    ACR TEXT,
    Department TEXT,
    Title TEXT,
    Age INT,
    ImageThumbnaillID BIGINT,
    Creation TIMESTAMP,
    WEBURL TEXT,
    Timestamp TIMESTAMP,
    sexe VARCHAR(10),
    severity_level VARCHAR(50),
    severity_confidence DECIMAL(5,4),
    Diagnosis_root TEXT,
    Organe_affecte TEXT,
    diagnosis_hash VARCHAR(64),
    patient_hash VARCHAR(64),
    department_hash VARCHAR(64)
);

-- STEP 3: Load CSV in PSQL


-- STEP 4: Compute hashes
UPDATE staging_medical_data 
SET diagnosis_hash = MD5(CONCAT(COALESCE(Diagnosis_root, ''), '|', COALESCE(severity_level, ''), '|', COALESCE(Organe_affecte, '')));

UPDATE staging_medical_data 
SET patient_hash = MD5(CONCAT(COALESCE(Age::text, 'NULL'), '|', COALESCE(sexe, '')));

UPDATE staging_medical_data 
SET department_hash = MD5(COALESCE(Department, ''));

-- STEP 5: Populate dimensions
INSERT INTO dim_diagnosis (diagnosis_group, severity_level, severity_confidence, organe_affecte, diagnosis_hash)
SELECT DISTINCT Diagnosis_root, severity_level, COALESCE(severity_confidence, 0), Organe_affecte, diagnosis_hash
FROM staging_medical_data WHERE diagnosis_hash IS NOT NULL
ON CONFLICT (diagnosis_hash) DO NOTHING;

INSERT INTO dim_patient (age, sexe, patient_hash)
SELECT DISTINCT Age, sexe, patient_hash
FROM staging_medical_data WHERE patient_hash IS NOT NULL
ON CONFLICT (patient_hash) DO NOTHING;

INSERT INTO dim_department (department, department_hash)
SELECT DISTINCT Department, department_hash
FROM staging_medical_data WHERE department_hash IS NOT NULL
ON CONFLICT (department_hash) DO NOTHING;

INSERT INTO dim_time (timestamp_date, year, month, day)
SELECT DISTINCT Timestamp::date, EXTRACT(YEAR FROM Timestamp)::INT, EXTRACT(MONTH FROM Timestamp)::INT, EXTRACT(DAY FROM Timestamp)::INT
FROM staging_medical_data WHERE Timestamp IS NOT NULL
ON CONFLICT (timestamp_date) DO NOTHING;

-- STEP 6: Populate fact table
INSERT INTO fact_medical_case (diagnosis_key, patient_key, department_key, time_key, nb_cases)
SELECT dd.diagnosis_key, dp.patient_key, dpt.department_key, dt.time_key, 1
FROM staging_medical_data s
JOIN dim_diagnosis dd ON dd.diagnosis_hash = s.diagnosis_hash
JOIN dim_patient dp ON dp.patient_hash = s.patient_hash
JOIN dim_department dpt ON dpt.department_hash = s.department_hash
JOIN dim_time dt ON dt.timestamp_date = s.Timestamp::date
WHERE s.diagnosis_hash IS NOT NULL AND s.patient_hash IS NOT NULL 
  AND s.department_hash IS NOT NULL AND s.Timestamp IS NOT NULL;

-- STEP 7: Verify
SELECT 'dim_diagnosis' as table_name, COUNT(*) FROM dim_diagnosis
UNION ALL SELECT 'dim_patient', COUNT(*) FROM dim_patient
UNION ALL SELECT 'dim_department', COUNT(*) FROM dim_department
UNION ALL SELECT 'dim_time', COUNT(*) FROM dim_time
UNION ALL SELECT 'fact_medical_case', COUNT(*) FROM fact_medical_case;

SELECT nb_cases from fact_medical_case;

SHOW CLIENT_ENCODING;

SHOW SERVER_ENCODING;

SELECT dim_diagnosis.diagnosis_group from dim_diagnosis;