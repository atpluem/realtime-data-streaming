-- CREATE DATABASE AND SCHEMA
CREATE DATABASE IF NOT EXISTS scd_customer;
CREATE SCHEMA IF NOT EXISTS customer;
USE DATABASE scd_customer;
USE SCHEMA customer;

-- CREATE TABLES
CREATE OR REPLACE TABLE customer(
    customer_id NUMBER,
    first_name STRING,
    last_name STRING,
    email STRING,
    street STRING,
    city STRING,
    state STRING,
    country STRING,
    update_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP());

CREATE OR REPLACE TABLE customer_hist(
    customer_id NUMBER,
    first_name STRING,
    last_name STRING,
    email STRING,
    street STRING,
    city STRING,
    state STRING,
    country STRING,
    start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    end_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    is_current BOOLEAN);

CREATE OR REPLACE TABLE customer_raw(
    customer_id NUMBER,
    first_name STRING,
    last_name STRING,
    email STRING,
    street STRING,
    city STRING,
    state STRING,
    country STRING);

-- CREATE STREAM
CREATE OR REPLACE STREAM customer_table_changes
ON TABLE customer;