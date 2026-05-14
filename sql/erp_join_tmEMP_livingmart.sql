FROM dbo.tmEMP AS A
INNER JOIN dbo.tmBRN AS B
    ON  A.fcBRNID = B.fcID
    AND A.fcBRNID = 'YOUR_BRANCH_ID'  -- [SCRUBBED] replace with your branch ID filter