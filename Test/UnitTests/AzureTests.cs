using SyncTK;
using System;
using Xunit;

namespace SyncTK.Test.UnitTests
{
    public class AzureTests : TestBase
    {
        [Fact]
        public void BlobToTSV()
        {
            new Sync()
                .From(new SourceAzureBlob(GetConfig("AzureBlobConnectionString"), GetConfig("AzureBlobContainer"), "Employee"))
                .WithFormat(new FormatParquet())
                .ConvertTo(new ConvertTSV(true))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\BlobToTSV_*.txt"))
                .Exec();
        }

        [Fact]
        public void SQLToBlobParquet()
        {
            new Sync()
                .From(new SourceSqlServer(@"(LocalDb)\MSSQLLocalDB", "SyncTK", "SELECT TOP 1000000 t.* FROM sys.objects t CROSS APPLY sys.objects a CROSS APPLY sys.objects b CROSS APPLY sys.objects c"))
                .ConvertTo(new ConvertParquet())
                .Into(new TargetAzureBlob(GetConfig("AzureBlobConnectionString"), GetConfig("AzureBlobContainer"), "SyncTKTest\\v1\\*.parquet"))
                .Exec();
        }
    }
}
