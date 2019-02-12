using SyncTK;
using System;
using Xunit;

namespace SyncTK.Test.UnitTests
{
    public class FileTests : TestBase
    {
        [Fact]
        public void TSVToTSVLocal()
        {
            new Sync()
                .From(new SourceFile($"{GetConfig("SampleFilesRoot")}\\*.txt"))
                .WithFormat(new FormatTSV(true))
                .ConvertTo(new ConvertTSV(true))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVToTSVLocal_*.txt"))
                .Exec();

        }

        [Fact]
        public void CSVToCSVLocalAsync()
        {
            var t = new Sync()
                .From(new SourceFile($"{GetConfig("SampleFilesRoot")}\\*.txt"))
                .WithFormat(new FormatTSV(true))
                .ConvertTo(new ConvertTSV(true))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\CSVToCSVLocalAsync_*.txt"))
                .ExecAsync();

            t.Start();
            t.Wait();
        }
    }
}