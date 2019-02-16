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
            new Pipeline()
                .From(new SourceFile($"{GetConfig("SampleFilesRoot")}\\*.txt"))
                .ReadFormat(new ReadTSV(true))
                .WriteFormat(new WriteTSV(true))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVToTSVLocal_*.txt"))
                .Exec();
        }

        [Fact]
        public void CSVToCSVLocalAsync()
        {
            var t = new Pipeline()
                .From(new SourceFile($"{GetConfig("SampleFilesRoot")}\\*.txt"))
                .ReadFormat(new ReadTSV(true))
                .WriteFormat(new WriteTSV(true))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\CSVToCSVLocalAsync_*.txt"))
                .ExecAsync();

            t.Start();
            t.Wait();
        }
    }
}