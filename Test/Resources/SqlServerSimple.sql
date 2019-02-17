SELECT TOP {0}
    CAST(200 AS TINYINT) [TinyInt]
    ,CAST(-20000 AS SMALLINT) [SmallInt]
    ,CAST(2000000000 AS INT) [Int]
    ,CAST(2000000000000000000 AS BIGINT) [BigInt]
    ,CAST(1.2345 AS FLOAT) [Float]
    ,CAST(1.2345 AS DECIMAL(38,17)) [Decimal]
    ,CAST(1.2345 AS NUMERIC(6, 3)) [Numeric]
    ,CAST(GETDATE() AS DATETIME) [DateTime]
    ,CAST(GETDATE() AS SMALLDATETIME) [SmallDateTime]
    ,CAST(GETDATE() AS DATETIME2) [DateTime2]
    ,CAST('This is a String' AS VARCHAR(100)) [VarChar]
    ,CAST('This is a String' AS CHAR(100)) [Char]
    ,CAST('This is a String' AS NVARCHAR(100)) [NVarChar]
    ,CAST('This is a String' AS NCHAR(100)) [NChar]
FROM sys.objects t
CROSS APPLY sys.objects a
CROSS APPLY sys.objects b
CROSS APPLY sys.objects c