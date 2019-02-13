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
    }
}
