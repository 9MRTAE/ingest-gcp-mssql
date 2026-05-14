FROM ERP.DBO.ttODH AS B
INNER JOIN ERP.DBO.ttODD AS A
    ON  A.fcODHID = B.fcID
    AND B.fcDCBID IN (
        SELECT fcID
        FROM   dbo.tmDCB
        WHERE  fcDCTID IN (
            SELECT fcID
            FROM   ERP.dbo.tmDCT
            WHERE  fcBRNID IN (
                SELECT fcID
                FROM   ERP.dbo.tmBRN
                WHERE  fcCOMID IN (
                    SELECT fcID
                    FROM   ERP.dbo.tmCOM
                    WHERE  rtrim(fcCode) = 'YOUR_COMPANY_CODE'  -- [SCRUBBED] replace with your company/tenant code
                       OR  rtrim(fcComShortCode) LIKE '%YOUR_ORG_CODE%'  -- [SCRUBBED] replace with your org short code filter
                )
                GROUP BY fcID
            )
        )
    )
