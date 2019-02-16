using SyncTK;
using System;
using Xunit;

namespace SyncTK.Test.UnitTests
{
    public class AzureTests : TestBase
    {
        //[Fact]
        //public void BlobToTSV()
        //{
        //    new Pipeline()
        //        .From(new SourceWASB(GetConfig("AzureBlobConnectionString"), GetConfig("AzureBlobContainer"), "Employee"))
        //        .ReadFormat(new ReadParquet())
        //        .WriteFormat(new WriteParquet())
        //        .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\BlobToTSV_*.txt"))
        //        .Exec();
        //}

        [Fact]
        public void SQLToBlobParquetSimple()
        {
            new Pipeline()
                .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", "SELECT TOP 1000000 t.* FROM sys.objects t CROSS APPLY sys.objects a CROSS APPLY sys.objects b CROSS APPLY sys.objects c"))
                .WriteFormat(new WriteParquet())
                .Into(new TargetWASB(GetConfig("AzureBlobConnectionString"), GetConfig("AzureBlobContainer"), "SyncTKTest\\v1\\SQLToBlobParquetSimple_*.parquet"))
                .Exec();
        }

        [Fact]
        public void SQLToBlobParquetLarge()
        {
            new Pipeline()
                .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", $"{SqlServerTests.QUERY_ALL_TYPES} CROSS APPLY sys.objects a"))
                .WriteFormat(new WriteParquet())
                .Into(new TargetWASB(GetConfig("AzureBlobConnectionString"), GetConfig("AzureBlobContainer"), "SyncTKTest\\v1\\SQLToBlobParquetLarge_*.parquet"))
                .Exec();
        }
    }
}