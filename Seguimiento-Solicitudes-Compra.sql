WITH DATOS AS (
    SELECT 
        T1.DocEntry, 
        SUM(T1.Quantity * T1.Price) AS SCompra, 
        T1.Currency AS MCompra, T1.DocDate,
        SUM(T2.Quantity * T2.Price) AS OfCompras, 
        T2.Currency AS MFCompras,
        SUM(T3.Quantity * T3.Price) AS OCompras, 
        T3.Currency AS MCompras,
        SUM(T4.Quantity * T4.Price) AS OEntrega, 
        T4.Currency AS MEntrega,
        SUM(T5.Quantity * T5.Price) AS FProvedor, 
        T5.Currency AS MProvedor
FROM PRQ1 T1 --Solicitud De Compra
FULL OUTER JOIN PQT1 T2 -- Oferta de Compra
ON T1.DocEntry = T2.BaseEntry AND T1.objType = T2.BaseType  AND T1.LineNum = T2.LineNum
FULL OUTER JOIN POR1 T3 --Orden de Compra
ON T1.DocEntry = T3.BaseEntry AND T1.objType = T3.BaseType  AND T1.LineNum = T3.LineNum AND T3.LineStatus = 'C' AND T3.TrgetEntry IS NOT NULL
OR T2.DocEntry = T3.BaseEntry AND T2.objType = T3.BaseType  AND T1.LineNum = T3.LineNum AND T3.LineStatus = 'C' AND T3.TrgetEntry IS NOT NULL
OR T1.DocEntry = T3.BaseEntry AND T1.objType = T3.BaseType  AND T1.LineNum = T3.LineNum AND T3.LineStatus = 'O'
OR T2.DocEntry = T3.BaseEntry AND T2.objType = T3.BaseType  AND T1.LineNum = T3.LineNum AND T3.LineStatus = 'O'
FULL OUTER JOIN PDN1 T4 --Entrega Mercancia
ON T3.DocEntry = T4.BaseEntry AND T3.objType = T4.BaseType AND T3.LineNum = T4.LineNum AND T4.LineStatus = 'C' AND T4.TrgetEntry IS NOT NULL AND T4.TargetType <> 21
OR T3.DocEntry = T4.BaseEntry AND T3.objType = T4.BaseType AND T3.LineNum = T4.LineNum AND T4.LineStatus = 'O'
FULL OUTER JOIN PCH1 T5 --Factura Provedor 
ON T4.DocEntry = T5.BaseEntry AND T4.objType = T5.BaseType AND T5.BaseType <> 18 AND T5.TargetType<> 18 AND T4.LineNum = T5.LineNum AND T5.LineStatus = 'C' AND T5.TrgetEntry IS NOT NULL
OR T4.DocEntry = T5.BaseEntry AND T4.objType = T5.BaseType AND T5.BaseType <> 18 AND T5.TargetType<> 18 AND T4.LineNum = T5.LineNum AND T5.LineStatus = 'O'
    GROUP BY T1.DocEntry, T1.Currency,T1.DocDate, T2.Currency, T3.Currency, T4.Currency, T5.Currency
),
DATOS_CON_ROWNUM AS (
    SELECT
        T0.DocEntry,
        SUM(SCompra) OVER (PARTITION BY DocEntry) AS SCompra,
        T0.MCompra,
        SUM(OfCompras) OVER (PARTITION BY DocEntry) AS OfCompras,
        T0.MFCompras,
        SUM(OCompras) OVER (PARTITION BY DocEntry) AS OCompras,
        T0.MCompras, 
        SUM(OEntrega) OVER (PARTITION BY DocEntry) AS OEntrega,
        T0.MEntrega,
        SUM(FProvedor) OVER (PARTITION BY DocEntry) FProvedor,
        T0.MProvedor,
        ROW_NUMBER() OVER (PARTITION BY T0.DocEntry ORDER BY T0.DocEntry) AS rn,
        T0.DocDate
    FROM DATOS T0
)
SELECT
    T0.DocEntry AS Documento,
    T0.SCompra SolicitudCompra,
    CASE WHEN T0.MCompra IS NULL 
    THEN (SELECT TOP(1) T1.MCompra FROM DATOS_CON_ROWNUM T1 WHERE T1.MCompra IS NOT NULL AND T1.DocEntry = T0.DocEntry)
    ELSE T0.MCompra END AS Moneda,
    T0.OfCompras AS OferntaCompra,
    CASE WHEN T0.MFCompras IS NULL 
    THEN (SELECT TOP(1) T1.MFCompras FROM DATOS_CON_ROWNUM T1 WHERE T1.MFCompras IS NOT NULL AND T1.DocEntry = T0.DocEntry)
    ELSE T0.MFCompras END AS Moneda,
    T0.OCompras AS OrdenCompra,
    CASE WHEN T0.MCompras  IS NULL 
    THEN (SELECT TOP(1) T1.MCompras FROM DATOS_CON_ROWNUM T1 WHERE T1.MCompras IS NOT NULL AND T1.DocEntry = T0.DocEntry)
    ELSE T0.MCompras  END AS Moneda, 
    T0.OEntrega Entrega,
    CASE WHEN T0.MEntrega IS NULL 
    THEN (SELECT TOP(1) T1.MEntrega FROM DATOS_CON_ROWNUM T1 WHERE T1.MEntrega IS NOT NULL AND T1.DocEntry = T0.DocEntry)
    ELSE T0.MEntrega END AS Moneda,
    T0.FProvedor FacturaProvedor,
    CASE WHEN T0.MProvedor IS NULL 
    THEN (SELECT TOP(1) T1.MProvedor FROM DATOS_CON_ROWNUM T1 WHERE T1.MProvedor IS NOT NULL AND T1.DocEntry = T0.DocEntry)
    ELSE T0.MProvedor END As Moneda
FROM DATOS_CON_ROWNUM T0
WHERE T0.rn = (SELECT MAX(rn) FROM DATOS_CON_ROWNUM T1 WHERE T1.DocEntry = T0.DocEntry) AND T0.DOcDate >= DATEADD(yy, DATEDIFF(yy, 0, GETDATE()), 0)
ORDER BY T0.DocEntry