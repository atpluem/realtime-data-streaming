USE DATABASE SCD_CUSTOMER;
USE SCHEMA CUSTOMER;

-- MERGE TABLES
MERGE INTO customer c
USING customer_raw cr
    ON c.customer_id = cr.customer_id
WHEN MATCHED AND
    c.customer_id <> cr.customer_id OR
    c.first_name  <> cr.first_name  OR
    c.last_name   <> cr.last_name   OR
    c.email       <> cr.email       OR
    c.street      <> cr.street      OR
    c.city        <> cr.city        OR
    c.state       <> cr.state       OR
    c.country     <> cr.country
    THEN UPDATE SET
    c.customer_id = cr.customer_id,
    c.first_name  = cr.first_name,
    c.last_name   = cr.last_name,
    c.email       = cr.email,
    c.street      = cr.street,
    c.city        = cr.city,
    c.state       = cr.state,
    c.country     = cr.country,
    update_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED
    THEN INSERT 
        (c.customer_id, c.first_name, c.last_name, c.email,
         c.street, c.city, c.state, c.country)
    VALUES 
        (cr.customer_id, cr.first_name, cr.last_name, cr.email,
         cr.street, cr.city, cr.state, cr.country);

-- CREATE MERGE STORE PROCEDURE
CREATE OR REPLACE PROCEDURE pdr_scd()
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
    $$
        var merge_table = `
            MERGE INTO customer c
            USING customer_raw cr
                ON c.customer_id = cr.customer_id
            WHEN MATCHED AND
                c.customer_id <> cr.customer_id OR
                c.first_name  <> cr.first_name  OR
                c.last_name   <> cr.last_name   OR
                c.email       <> cr.email       OR
                c.street      <> cr.street      OR
                c.city        <> cr.city        OR
                c.state       <> cr.state       OR
                c.country     <> cr.country
                THEN UPDATE SET
                c.customer_id = cr.customer_id,
                c.first_name  = cr.first_name,
                c.last_name   = cr.last_name,
                c.email       = cr.email,
                c.street      = cr.street,
                c.city        = cr.city,
                c.state       = cr.state,
                c.country     = cr.country,
                update_timestamp = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED
                THEN INSERT 
                    (c.customer_id, c.first_name, c.last_name, c.email,
                     c.street, c.city, c.state, c.country)
                VALUES 
                    (cr.customer_id, cr.first_name, cr.last_name, cr.email,
                     cr.street, cr.city, cr.state, cr.country);
        `
        var empty_raw = "TRUNCATE TABLE SCD_CUSTOMER.CUSTOMER.customer_raw;"
        var sql1 = snowflake.createStatement({sqlText: merge_table});
        var sql2 = snowflake.createStatement({sqlText: empty_raw});
        var result1 = sql1.execute();
        var result2 = sql2.execute();
    return merge_table + '\n' + empty_raw;
    $$;

-- SETUP A ROLE FOR TASK
USE ROLE SECURITYADMIN;
CREATE OR REPLACE ROLE taskadmin;

USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE taskadmin;

-- CREATE MERGE CUSTOMER TABLE TASK
CREATE OR REPLACE TASK tsk_scd_raw
    WAREHOUSE = COMPUTE_WH schedule = '1 minute'
    ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
    AS 
        CALL pdr_scd();

-- START/STOP TASK
ALTER TASK tsk_scd_raw RESUME; -- SUSPEND
SHOW TASKS;