using SyncTK;
using System;
using Xunit;
using Xunit.Abstractions;

namespace SyncTK.Test.UnitTests
{
    public class FileTests : TestBase
    {
        public FileTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TSVToParquetSimple()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVSimple*.txt"))
                .ReadFormat(new ReadTSV(true))
                .WriteFormat(new WriteParquet())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVToParquetSimple*.parquet"))
                .Exec());
        }

        [Fact]
        public void TSVToParquetComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVComplex*.txt"))
                .ReadFormat(new ReadTSV(true))
                .WriteFormat(new WriteParquet())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVToParquetComplex*.parquet"))
                .Exec());
        }

        [Fact]
        public void ParquetToTSVSimple()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetSimple*.parquet"))
                .ReadFormat(new ReadParquet())
                .WriteFormat(new WriteTSV())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\ParquetToTSVSimple*.txt"))
                .Exec());
        }

        [Fact]
        public void ParquetToTSVComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetSimple*.parquet"))
                .ReadFormat(new ReadParquet())
                .WriteFormat(new WriteParquet())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\ParquetToTSVSimple*.txt"))
                .Exec());
        }


        [Fact]
        public void TSVToParquetUncompressed()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVComplex*.txt"))
                .ReadFormat(new ReadTSV(true))
                .WriteFormat(new WriteParquet(false))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVToParquetUncompressed*.parquet"))
                .Exec());
        }
    }
}