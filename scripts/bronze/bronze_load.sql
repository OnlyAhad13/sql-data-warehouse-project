CALL bronze.load_bronze();

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
    LANGUAGE plpgsql
AS
$$
DECLARE
    starttime       TIMESTAMP;
    endtime         TIMESTAMP;
    duration        INTERVAL;
    table_starttime TIMESTAMP;
    table_endtime   TIMESTAMP;
    table_duration  INTERVAL;
BEGIN
    -- Capture Overall Time
    starttime := now();
    RAISE NOTICE 'ETL Started at: %', starttime;

    -- CRM Customer Info
    table_starttime := now();
    RAISE NOTICE 'Truncating table bronze.crm_cust_info...';
    TRUNCATE TABLE bronze.crm_cust_info;

    RAISE NOTICE 'Loading data into bronze.crm_cust_info...';
    COPY bronze.crm_cust_info FROM '/tmp/cust_info.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');
    table_endtime := now();
    table_duration := table_endtime - table_starttime;
    RAISE NOTICE 'Time taken for crm_cust_info: %', table_duration;

    -- CRM Product Info
    table_starttime := now();
    RAISE NOTICE 'Truncating table bronze.crm_prd_info...';
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE NOTICE 'Loading data into bronze.crm_prd_info...';
    COPY bronze.crm_prd_info FROM '/tmp/prd_info.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');
    table_endtime := now();
    table_duration := table_endtime - table_starttime;
    RAISE NOTICE 'Time taken for crm_prd_info: %', table_duration;

    -- CRM Sales Details
    table_starttime := now();
    RAISE NOTICE 'Truncating table bronze.crm_sales_details...';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE 'Loading data into bronze.crm_sales_details...';
    COPY bronze.crm_sales_details FROM '/tmp/sales_details.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');
    table_endtime := now();
    table_duration := table_endtime - table_starttime;
    RAISE NOTICE 'Time taken for crm_sales_details: %', table_duration;

    -- ERP Location A101
    table_starttime := now();
    RAISE NOTICE 'Truncating table bronze.erp_loc_a101...';
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE NOTICE 'Loading data into bronze.erp_loc_a101...';
    COPY bronze.erp_loc_a101 FROM '/tmp/LOC_A101.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');
    table_endtime := now();
    table_duration := table_endtime - table_starttime;
    RAISE NOTICE 'Time taken for erp_loc_a101: %', table_duration;

    -- ERP Customer AZ12
    table_starttime := now();
    RAISE NOTICE 'Truncating table bronze.erp_cust_az12...';
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE NOTICE 'Loading data into bronze.erp_cust_az12...';
    COPY bronze.erp_cust_az12 FROM '/tmp/CUST_AZ12.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');
    table_endtime := now();
    table_duration := table_endtime - table_starttime;
    RAISE NOTICE 'Time taken for erp_cust_az12: %', table_duration;

    -- ERP PX Category G1V2
    table_starttime := now();
    RAISE NOTICE 'Truncating table bronze.erp_px_cat_g1v2...';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE NOTICE 'Loading data into bronze.erp_px_cat_g1v2...';
    COPY bronze.erp_px_cat_g1v2 FROM '/tmp/PX_CAT_G1V2.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');
    table_endtime := now();
    table_duration := table_endtime - table_starttime;
    RAISE NOTICE 'Time taken for erp_px_cat_g1v2: %', table_duration;

    -- Capture Overall ETL End Time
    endtime := now();
    duration := endtime - starttime;

    RAISE NOTICE 'Ended at:  %', endtime;
    RAISE NOTICE 'Total Duration: %', duration;
END;
$$;
