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

        [Fact]
        public void TSVToParquetLarge()
        {
            new Pipeline()
                .From(new SourceFile($"{GetConfig("SampleFilesRoot")}\\Sample10000.txt"))
                .ReadFormat(new ReadTSV(true))
                .WriteFormat(new WriteParquet())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVToParquetLarge_*.parquet"))
                .Exec();
        }

        [Fact]
        public void OddParquetLargeToParquetUncompressed()
        {
            new Pipeline()
                .From(new SourceFile($"{GetConfig("SampleFilesRoot")}\\OddTypesLarge.parquet"))
                .ReadFormat(new ReadParquet())
                .WriteFormat(new WriteParquet(true))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\OddParquetLargeToParquetUncompressed_*.parquet"))
                .Exec();
        }

        [Fact]
        public void ParquetToTSVLarge()
        {
            new Pipeline()
                .From(new SourceFile($"{GetConfig("SampleFilesRoot")}\\Sample10000.parquet"))
                .ReadFormat(new ReadParquet())
                .WriteFormat(new WriteTSV(true))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\ParquetToTSVLarge_*.txt"))
                .Exec();
        }
    }
}