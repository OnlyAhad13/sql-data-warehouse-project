/*
 If facing permission issues, move the datasets to tmp folder in linux or equivalent in windows
 or MacOS.
 */
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
    LANGUAGE plpgsql
AS
$$
BEGIN
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
        FROM '/tmp/cust_info.csv'
        WITH (
        FORMAT CSV,
        HEADER,
        DELIMITER ','
        );

    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
        FROM '/tmp/prd_info.csv'
        WITH (
        FORMAT CSV,
        HEADER,
        DELIMITER ','
        );

    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
        FROM '/tmp/sales_details.csv'
        WITH (
        FORMAT CSV,
        HEADER,
        DELIMITER ','
        );

    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
        FROM '/tmp/LOC_A101.csv'
        WITH (
        FORMAT CSV,
        HEADER,
        DELIMITER ','
        );

    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
        FROM '/tmp/CUST_AZ12.csv'
        WITH (
        FORMAT CSV,
        HEADER,
        DELIMITER ','
        );

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
        FROM '/tmp/PX_CAT_G1V2.csv'
        WITH (
        FORMAT CSV,
        HEADER,
        DELIMITER ','
        );
END;
$$;