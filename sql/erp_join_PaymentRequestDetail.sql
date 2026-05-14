FROM ERP.DBO.PaymentRequestDetail AS A
INNER JOIN ERP.DBO.PaymentRequestHeader AS B
    ON B.id = A.PaymentRequestHeaderId
