using SyncTK;
using System;
using Xunit;

namespace SyncTK.Test.UnitTests
{
    public class SqlServerTests : TestBase
    {
        public const string QUERY_ALL_TYPES = @"
            SELECT
                NEWID() [UniqueIdentifier]
                ,CAST(2000000000 AS INT) [Int]
                ,CAST(2000000000000000000 AS BIGINT) [BigInt]
                ,CAST(1.2345 AS FLOAT) [Float]
                ,CAST(1.2345 AS DECIMAL(38,17)) [Decimal]
                ,CAST(1.2345 AS NUMERIC(6, 3)) [Numeric]
                ,CAST(1.2345 AS REAL) [Real]
                ,geography::Point(47.65100, -122.34900, 4326) [Geography]
                ,geometry::STGeomFromText('LINESTRING (100 100, 20 180, 180 180)', 0) [Geometry]
                ,CAST('<xml><hello>world</hello></xml>' AS XML) [Xml]
                ,CAST('Hello World' AS VARBINARY(100)) [VarBinary]
                ,CAST('Hello World' AS BINARY(100)) [Binary]
                ,CAST(GETDATE() AS DATETIME) [DateTime]
                ,CAST(GETDATE() AS SMALLDATETIME) [SmallDateTime]
                ,CAST(GETDATE() AS DATETIME2) [DateTime2]
                ,CAST(GETDATE() AS DATETIMEOFFSET) [DateTimeOffset]
                ,CAST(GETDATE() AS TIME) [Time]
                ,CAST('/1/' AS HIERARCHYID) [HierarchyID]
                ,CAST(12.99 AS SMALLMONEY) [SmallMoney]
                ,CAST(12.99 AS MONEY) [Money]
                ,CAST(GETDATE() AS TIMESTAMP) [TimeStamp]
                ,'No Column Name'
                ,CAST('This is a String' AS VARCHAR(100)) [VarChar]
                ,CAST('This is a String' AS CHAR(100)) [Char]
                ,CAST('This is a String' AS NVARCHAR(100)) [NVarChar]
                ,CAST('This is a String' AS NCHAR(100)) [NChar]
                ,CAST('This is a String' AS TEXT) [Text]
                ,CAST('This is a String' AS SQL_VARIANT) [Variant]
            FROM sys.objects t";

        //[Fact]
        //public void DBToTSVSimple()
        //{
        //    new Pipeline()
        //        .From(new SourceSqlServer(GetConfig("SQLServer").ToString(), "SyncTK", "sys", "objects"))
        //        .WriteFormat(new WriteTSV(true))
        //        .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\DBToTSVSimple_*.txt"))
        //        .Exec();
        //}

        //[Fact]
        //public void DBToTSV()
        //{
        //    new Pipeline()
        //        .From(new SourceSqlServer(GetConfig("SQLServer").ToString(), "SyncTK", QUERY_ALL_TYPES))
        //        .WriteFormat(new WriteTSV(true))
        //        .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\DBToTSV_*.txt"))
        //        .Exec();
        //}

        //[Fact]
        //public void TSVToDBSample()
        //{
        //    new Pipeline()
        //        .From(new SourceFile($"{GetConfig("SampleFilesRoot")}\\Sample10000.txt"))
        //        .ReadFormat(new ReadTSV(true))
        //        .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "TSVToDBSample", true))
        //        .Exec();
        //}

        //[Fact]
        //public void DBToDB()
        //{
        //    new Pipeline()
        //        .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", QUERY_ALL_TYPES))
        //        .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "DBToDB", true))
        //        .Exec();
        //}

        //[Fact]
        //public void DBToDBLarge()
        //{
        //    new Pipeline()
        //        .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", $"{QUERY_ALL_TYPES} CROSS APPLY sys.objects a"))      //  CROSS APPLY sys.objects b
        //        .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "DBToDBLarge", true))
        //        .Exec();
        //}

        //[Fact]
        //public void DBToParquetSimple()
        //{
        //    new Pipeline()
        //        .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "sysobjects"))
        //        .WriteFormat(new WriteParquet())
        //        .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\DBToParquetSmall_*.parquet"))
        //        .Exec();
        //}

        //[Fact]
        //public void DBToParquetLarge()
        //{
        //    new Pipeline()
        //        .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", $"{QUERY_ALL_TYPES} CROSS APPLY sys.objects a CROSS APPLY sys.objects b"))
        //        .WriteFormat(new WriteParquet())
        //        .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\DBToParquetLarge_*.parquet"))
        //        .Exec();
        //}

        [Fact]
        public void DBToParquetMultiple()
        {
            new Pipeline()
                .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", $"SELECT TOP 1000000 * FROM sys.objects o CROSS APPLY sys.objects a CROSS APPLY sys.objects b CROSS APPLY sys.objects c"))
                .WriteFormat(new WriteParquet(100000, 4000000))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\DBToParquetLarge_*.parquet"))
                .Exec();
        }
    }
}