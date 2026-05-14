FROM ERP.DBO.tmCOR AS A WITH(NOLOCK)
INNER JOIN ERP.DBO.tmPRO AS B WITH(NOLOCK)
    ON  rtrim(A.fcPROFEE) = rtrim(B.fcID)
    AND rtrim(A.fcCOMID)  = rtrim(B.fcCOMID)
    AND A.fcisecom = 'Y'
 