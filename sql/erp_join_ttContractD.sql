FROM ERP.DBO.ttContractD AS A
INNER JOIN ERP.DBO.ttContractH AS B
    ON B.fcID = A.fcContractHID
