USE DATABASE SCD_CUSTOMER;
USE SCHEMA CUSTOMER;

-- CREATE VIEW
CREATE OR REPLACE VIEW v_customer_change_data
AS
    -- inserted event
    SELECT customer_id, first_name, last_name, email,
        street, city, state, country,
        start_time, end_time, is_current, 'I' AS dml_type
    FROM (
        SELECT customer_id, first_name, last_name, email, 
            street, city, state, country,
            update_timestamp AS start_time,
            LAG(update_timestamp) OVER (
                PARTITION BY customer_id 
                ORDER BY update_timestamp DESC) AS end_time_raw,
            CASE WHEN end_time_raw IS NULL THEN '9999-12-31'::TIMESTAMP_NTZ
                ELSE end_time_raw END AS end_time,
            CASE WHEN end_time_raw IS NULL THEN TRUE
                ELSE FALSE END AS is_current
        FROM (
              SELECT customer_id, first_name, last_name, email,
                     street, city, state, country, update_timestamp
              FROM customer_table_changes
              WHERE metadata$action = 'INSERT' AND
                  metadata$isupdate = 'FALSE'
        )   
    )
    UNION
    -- updated event
    SELECT customer_id, first_name, last_name, email, 
        street, city, state, country,
        start_time, end_time, is_current, dml_type
    FROM (
        SELECT customer_id, first_name, last_name, email, 
            street, city, state, country,
            update_timestamp AS start_time,
            LAG(update_timestamp) OVER (
                PARTITION BY customer_id 
                ORDER BY update_timestamp DESC) AS end_time_raw,
            CASE WHEN end_time_raw IS NULL THEN '9999-12-31'::TIMESTAMP_NTZ
                ELSE end_time_raw END AS end_time,
            CASE WHEN end_time_raw IS NULL THEN TRUE
                ELSE FALSE END AS is_current,
            dml_type
        FROM (
            -- identify data to insert into customer_hist table
            SELECT customer_id, first_name, last_name, email,
                street, city, state, country, 
                update_timestamp, 'I' AS dml_type
            FROM customer_table_changes
            WHERE metadata$action = 'INSERT' AND
                metadata$isupdate = 'TRUE'
            UNION
            -- identify data in customer_hist table that need to be updated
            SELECT customer_id, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, 
                start_time, 'U' AS dml_type
            FROM customer_hist
            WHERE customer_id IN (
                SELECT DISTINCT customer_id
                FROM customer_table_changes
                WHERE metadata$action = 'DELETE' AND
                    metadata$isupdate = 'TRUE') AND
                is_current = TRUE
        )
    )
    UNION
    -- deleted event
    SELECT ctc.customer_id, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL,
        ch.start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, NULL, 'D'
    FROM customer_hist ch
    INNER JOIN customer_table_changes ctc
        ON ch.customer_id = ctc.customer_id
    WHERE ctc.metadata$action = 'DELETE' AND
        ctc.metadata$isupdate = 'FALSE' AND
        ch.is_current = TRUE;

SELECT * FROM v_customer_change_data;

-- CREATE CUSTOMER TABLE CHANGE TASK
CREATE OR REPLACE TASK tsk_scd_hist
    WAREHOUSE = COMPUTE_WH schedule = '1 minute'
    ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
    AS 
        MERGE INTO customer_hist ch
        USING v_customer_change_data ccd
            ON ch.customer_id = ccd.customer_id AND
                ch.start_time = ccd.start_time
        WHEN MATCHED AND ccd.dml_type = 'U' THEN UPDATE
            SET ch.end_time = ccd.end_time,
                ch.is_current = FALSE
        WHEN MATCHED AND ccd.dml_type = 'D' THEN UPDATE
            SET ch.end_time = ccd.end_time,
                ch.is_current = FALSE
        WHEN NOT MATCHED AND ccd.dml_type = 'I' THEN INSERT
            (customer_id, first_name, last_name, email, 
            street, city, state, country, 
            start_time, end_time, is_current)
            VALUES 
            (ccd.customer_id, ccd.first_name, ccd.last_name, ccd.email, 
            ccd.street, ccd.city, ccd.state, ccd.country, 
            ccd.start_time, ccd.end_time, ccd.is_current);

-- START/STOP TASK
ALTER TASK tsk_scd_hist SUSPEND; -- SUSPEND RESUME
SHOW TASKS;

-- EXAMPLE DML
INSERT INTO customer VALUES(223136,'Jessica','Arnold','tanner39@smith.com',
    '595 Benjamin Forge Suite 124','Michaelstad','Connecticut',
    'Cape Verde', CURRENT_TIMESTAMP());
UPDATE customer SET FIRST_NAME='Lufy' WHERE customer_id=7523;
DELETE FROM customer WHERE customer_id = 223136 AND FIRST_NAME = 'Jessica';
SELECT * FROM customer_hist WHERE customer_id = 7523;
SELECT * FROM customer_hist WHERE IS_CURRENT = FALSE;