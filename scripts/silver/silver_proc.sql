CREATE OR ALTER PROCEDURE silver.load_silver 
AS
BEGIN
    SET NOCOUNT ON;

    -- =========================================================
    -- crm_cust_info  (cast_key en destino)
    -- =========================================================
    TRUNCATE TABLE silver.crm_cust_info;

    INSERT INTO silver.crm_cust_info(
        cst_id,
        cast_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT 
        cst_id,
        cast_key,   -- <-- nombre correcto según tu DDL
        LTRIM(RTRIM(cst_firstname)) AS cst_firstname,
        LTRIM(RTRIM(cst_lastname))  AS cst_lastname,
        CASE UPPER(LTRIM(RTRIM(cst_marital_status)))
            WHEN 'S' THEN 'Single'
            WHEN 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE UPPER(LTRIM(RTRIM(cst_gndr)))
            WHEN 'F' THEN 'Female'
            WHEN 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        TRY_CONVERT(date, cst_create_date) AS cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
    ) t
    WHERE flag_last = 1
      AND cst_id IS NOT NULL;

    -- =========================================================
    -- crm_prd_info  (prd_cost INT en destino)
    -- =========================================================
    TRUNCATE TABLE silver.crm_prd_info;

    ;WITH src AS (
        SELECT
            prd_id,
            prd_key,
            prd_nm,
            TRY_CONVERT(int, prd_cost)                              AS prd_cost_i,   -- destino INT
            UPPER(LTRIM(RTRIM(prd_line)))                           AS prd_line_raw,
            TRY_CONVERT(date, prd_start_dt)                         AS prd_start_dt_d
        FROM bronze.crm_prd_info
    )
    INSERT INTO silver.crm_prd_info(
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')                 AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key))                         AS prd_key,
        prd_nm,
        ISNULL(prd_cost_i, 0)                                       AS prd_cost,
        CASE prd_line_raw
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END                                                         AS prd_line,
        prd_start_dt_d                                              AS prd_start_dt,
        DATEADD(DAY, -1, LEAD(prd_start_dt_d) OVER (
            PARTITION BY prd_key ORDER BY prd_start_dt_d
        ))                                                          AS prd_end_dt
    FROM src;

    -- =========================================================
    -- crm_sales_details  (fechas INT; ventas/precio INT)
    -- =========================================================
    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT 
        LTRIM(RTRIM(CAST(sls_ord_num AS NVARCHAR(50))))             AS sls_ord_num,    -- destino NVARCHAR(50)
        LTRIM(RTRIM(CAST(sls_prd_key AS NVARCHAR(50))))             AS sls_prd_key,    -- destino NVARCHAR(50)
        TRY_CONVERT(int, sls_cust_id)                               AS sls_cust_id,

        -- Fechas como INT (YYYYMMDD) si son 8 dígitos válidos; si no, NULL
        CASE 
          WHEN sls_order_dt IS NULL THEN NULL
          WHEN LEN(CAST(sls_order_dt AS VARCHAR(8))) <> 8 THEN NULL
          WHEN TRY_CONVERT(int, sls_order_dt) IS NULL THEN NULL
          ELSE TRY_CONVERT(int, sls_order_dt)
        END AS sls_order_dt,
        CASE 
          WHEN sls_ship_dt IS NULL THEN NULL
          WHEN LEN(CAST(sls_ship_dt AS VARCHAR(8))) <> 8 THEN NULL
          WHEN TRY_CONVERT(int, sls_ship_dt) IS NULL THEN NULL
          ELSE TRY_CONVERT(int, sls_ship_dt)
        END AS sls_ship_dt,
        CASE 
          WHEN sls_due_dt IS NULL THEN NULL
          WHEN LEN(CAST(sls_due_dt AS VARCHAR(8))) <> 8 THEN NULL
          WHEN TRY_CONVERT(int, sls_due_dt) IS NULL THEN NULL
          ELSE TRY_CONVERT(int, sls_due_dt)
        END AS sls_due_dt,

        -- Ventas y precio INT en destino
        -- Si sls_sales está mal o no coincide: recalc = qty * ABS(price)
        CASE 
          WHEN TRY_CONVERT(int, sls_sales) IS NULL 
               OR TRY_CONVERT(int, sls_sales) <= 0
               OR TRY_CONVERT(int, sls_sales) 
                    <> TRY_CONVERT(int, sls_quantity) * ABS(TRY_CONVERT(int, sls_price))
          THEN ISNULL(TRY_CONVERT(int, sls_quantity),0) * ABS(ISNULL(TRY_CONVERT(int, sls_price),0))
          ELSE TRY_CONVERT(int, sls_sales)
        END AS sls_sales,

        ISNULL(TRY_CONVERT(int, sls_quantity), 0)                   AS sls_quantity,

        CASE 
          WHEN TRY_CONVERT(int, sls_price) IS NULL 
               OR TRY_CONVERT(int, sls_price) <= 0
          THEN 
               -- derivar precio = ventas / cantidad; redondear a entero
               CASE WHEN NULLIF(TRY_CONVERT(int, sls_quantity), 0) IS NULL THEN NULL
                    ELSE TRY_CONVERT(int, ROUND(
                           1.0 * ISNULL(TRY_CONVERT(int, sls_sales), 
                                        ISNULL(TRY_CONVERT(int, sls_quantity),0) * ABS(ISNULL(TRY_CONVERT(int, sls_price),0)))
                           / NULLIF(TRY_CONVERT(int, sls_quantity), 0), 0))
               END
          ELSE TRY_CONVERT(int, sls_price)
        END AS sls_price
    FROM bronze.crm_sales_details;

    -- =========================================================
    -- erp_cust_az12
    -- =========================================================
    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END AS cid,
        CASE WHEN TRY_CONVERT(date, bdate) > GETDATE() THEN NULL ELSE TRY_CONVERT(date, bdate) END AS bdate,
        CASE 
            WHEN UPPER(LTRIM(RTRIM(gen))) IN ('F','FEMALE') THEN 'Female'
            WHEN UPPER(LTRIM(RTRIM(gen))) IN ('M','MALE')   THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;

    -- =========================================================
    -- erp_loc_a101
    -- =========================================================
    TRUNCATE TABLE silver.erp_loc_a101;

    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE
            WHEN LTRIM(RTRIM(cntry)) = 'DE'              THEN 'Germany'
            WHEN LTRIM(RTRIM(cntry)) IN ('US','USA')     THEN 'United States'
            WHEN LTRIM(RTRIM(cntry)) = '' OR cntry IS NULL THEN 'n/a'
            ELSE LTRIM(RTRIM(cntry))
        END AS cntry
    FROM bronze.erp_loc_a101;

    -- =========================================================
    -- erp_px_cat_g1v2
    -- =========================================================
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT 
        CAST(id AS NVARCHAR(50)) AS id,
        CAST(cat AS NVARCHAR(50)),
        CAST(subcat AS NVARCHAR(50)),
        CAST(maintenance AS NVARCHAR(50))
    FROM bronze.erp_px_cat_g1v2;
END;
GO

-- Ejecuta
EXEC silver.load_silver;
