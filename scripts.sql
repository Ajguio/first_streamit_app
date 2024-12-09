SELECT CURRENT_ACCOUNT();

---> set the Role
USE ROLE accountadmin;

---> set the Warehouse
USE WAREHOUSE compute_wh;

--PASO EN AWS: Crear un rol "SnowflakeIntegrationRole " para establecer relacion de confianza entre AWS y snowflake
-- Con el ARN del rol, creo una integracion, asi:

CREATE OR REPLACE STORAGE INTEGRATION my_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::195275668159:role/SnowflakeIntegrationRole'
STORAGE_ALLOWED_LOCATIONS = ('s3://glchallenge/filesemployees/');


-- Crear una nueva base de datos
CREATE OR REPLACE DATABASE company_data;

-- Usar la nueva base de datos
USE DATABASE company_data;

-- Crear un nuevo esquema
CREATE SCHEMA hiring_data;

-- Usar el esquema
USE SCHEMA hiring_data;


-- Tabla departments
--CREATE OR REPLACE TABLE company_data.hiring_data.departments (
CREATE OR REPLACE TABLE departments (
    id INTEGER PRIMARY KEY,    -- Primary Key para identificar de forma única el departamento
    department STRING          -- Nombre del departamento
);

-- Tabla jobs
CREATE OR REPLACE TABLE jobs (
    id INTEGER PRIMARY KEY,    -- Primary Key para identificar de forma única el trabajo
    job STRING                 -- Nombre del trabajo
);

-- Tabla hired_employees
CREATE OR REPLACE TABLE hired_employees (
    id INTEGER PRIMARY KEY,        -- Primary Key para identificar de forma única al empleado
    name STRING,                   -- Nombre y apellido del empleado
    datetime TIMESTAMP_NTZ,        -- Fecha y hora de contratación
    department_id INTEGER,         -- Foreign Key hacia departments
    job_id INTEGER,                -- Foreign Key hacia jobs
    CONSTRAINT fk_department FOREIGN KEY (department_id) REFERENCES departments(id),
    CONSTRAINT fk_job FOREIGN KEY (job_id) REFERENCES jobs(id)
);


-- Stage para la tabla departments
CREATE OR REPLACE STAGE stage_departments
STORAGE_INTEGRATION = my_s3_integration
URL = 's3://glchallenge/filesemployees/departments/';

-- Stage para la tabla jobs
CREATE OR REPLACE STAGE stage_jobs
STORAGE_INTEGRATION = my_s3_integration
URL = 's3://glchallenge/filesemployees/jobs/';

-- Stage para la tabla hired_employees
CREATE OR REPLACE STAGE stage_hired_employees
STORAGE_INTEGRATION = my_s3_integration
URL = 's3://glchallenge/filesemployees/hired_employees/';


LIST @stage_departments;

LIST @stage_jobs;

LIST @stage_hired_employees;


select $1, $2 
from @stage_departments/departments.csv

-- Cargar datos en la tabla departments
COPY INTO departments
FROM @stage_departments/departments.csv
FILE_FORMAT = (TYPE = 'CSV' /*FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1*/);

-- Cargar datos en la tabla jobs
COPY INTO jobs
FROM @stage_jobs/jobs.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' /*SKIP_HEADER = 1*/);

-- Cargar datos en la tabla hired_employees
COPY INTO hired_employees
FROM @stage_hired_employees/hired_employees.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"'  /*SKIP_HEADER = 1*/);


select * from departments;
TRUNCATE TABLE departments;

select * from jobs;
TRUNCATE TABLE jobs;

select * from hired_employees;
TRUNCATE TABLE hired_employees;


-- empleados y sus departamentos
SELECT e.id AS employee_id, e.name, e.datetime, d.department, j.job
FROM hired_employees e
JOIN departments d ON e.department_id = d.id
JOIN jobs j ON e.job_id = j.id;

--Número de empleados por departamento:

SELECT d.department, COUNT(e.id) AS total_employees
FROM hired_employees e
JOIN departments d ON e.department_id = d.id
GROUP BY d.department
ORDER BY total_employees DESC;


/*Number of employees hired for each job and department in 2021 divided by quarter. The
table must be ordered alphabetically by department and job.*/


SELECT 
    d.department AS department_name,
    j.job AS job_name,
    SUM(CASE WHEN EXTRACT(QUARTER FROM e.datetime) = 1 THEN 1 ELSE 0 END) AS Q1,
    SUM(CASE WHEN EXTRACT(QUARTER FROM e.datetime) = 2 THEN 1 ELSE 0 END) AS Q2,
    SUM(CASE WHEN EXTRACT(QUARTER FROM e.datetime) = 3 THEN 1 ELSE 0 END) AS Q3,
    SUM(CASE WHEN EXTRACT(QUARTER FROM e.datetime) = 4 THEN 1 ELSE 0 END) AS Q4
FROM 
    hired_employees e
JOIN 
    departments d ON e.department_id = d.id
JOIN 
    jobs j ON e.job_id = j.id
WHERE 
    EXTRACT(YEAR FROM e.datetime) = 2021
GROUP BY 
    d.department, j.job
ORDER BY 
    d.department ASC, 
    j.job ASC;

/*List of ids, name and number of employees hired of each department that hired more
employees than the mean of employees hired in 2021 for all the departments, ordered
by the number of employees hired (descending).*/
--Common Table Expressions (CTEs)   
WITH department_hires AS (
    -- Número de empleados contratados por cada departamento en 2021
    SELECT 
        d.id AS department_id,
        d.department AS department_name,
        COUNT(e.id) AS total_hires
    FROM 
        hired_employees e
    JOIN 
        departments d ON e.department_id = d.id
    WHERE 
        EXTRACT(YEAR FROM e.datetime) = 2021
    GROUP BY 
        d.id, d.department
),
average_hires AS (
    -- Promedio de contrataciones en 2021
    SELECT 
        AVG(total_hires) AS mean_hires
    FROM 
        department_hires
)
-- Departamentos con más contrataciones que el promedio
SELECT 
    dh.department_id,
    dh.department_name,
    dh.total_hires
FROM 
    department_hires dh
CROSS JOIN 
    average_hires ah
WHERE 
    dh.total_hires > ah.mean_hires
ORDER BY 
    dh.total_hires DESC;

    


