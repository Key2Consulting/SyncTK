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
        public void TSVToParquetUncompressed()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVComplex*.txt"))
                .ReadFormat(new ReadTSV(true))
                .WriteFormat(new WriteParquet(false))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVToParquetUncompressed*.parquet"))
                .Exec());
        }

        [Fact]
        public void TSVToCSVSimple()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVSimple*.txt"))
                .ReadFormat(new ReadTSV(true))
                .WriteFormat(new WriteCSV(false))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVToCSVSimple*.csv"))
                .Exec());
        }

        [Fact]
        public void TSVToCSVComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVComplex*.txt"))
                .ReadFormat(new ReadTSV(true))
                .WriteFormat(new WriteCSV(false))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVToCSVComplex*.csv"))
                .Exec());
        }

        [Fact]
        public void TSVToJSONSimple()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVSimple*.txt"))
                .ReadFormat(new ReadTSV(true))
                .WriteFormat(new WriteJSON())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVToJSONSimple*.json"))
                .Exec());
        }

        [Fact]
        public void TSVToJSONComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVComplex*.txt"))
                .ReadFormat(new ReadTSV(true))
                .WriteFormat(new WriteJSON())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVToJSONComplex*.json"))
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
                .WriteFormat(new WriteTSV())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\ParquetToTSVComplex*.txt"))
                .Exec());
        }

        [Fact]
        public void ParquetToCSVSimple()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetSimple*.parquet"))
                .ReadFormat(new ReadParquet())
                .WriteFormat(new WriteCSV())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\ParquetToCSVSimple*.csv"))
                .Exec());
        }

        [Fact]
        public void ParquetToCSVComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetSimple*.parquet"))
                .ReadFormat(new ReadParquet())
                .WriteFormat(new WriteCSV())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\ParquetToCSVComplex*.csv"))
                .Exec());
        }

        [Fact]
        public void ParquetToJSONSimple()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetSimple*.parquet"))
                .ReadFormat(new ReadParquet())
                .WriteFormat(new WriteJSON())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\ParquetToJSONSimple*.json"))
                .Exec());
        }

        [Fact]
        public void ParquetToJSONComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetSimple*.parquet"))
                .ReadFormat(new ReadParquet())
                .WriteFormat(new WriteJSON())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\ParquetToJSONComplex*.json"))
                .Exec());
        }

        [Fact]
        public void CSVToTSVSimple()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\CSVSimple*.csv"))
                .ReadFormat(new ReadCSV(true))
                .WriteFormat(new WriteTSV(false))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\CSVToTSVSimple*.txt"))
                .Exec());
        }

        [Fact]
        public void CSVToTSVComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\CSVComplex*.csv"))
                .ReadFormat(new ReadCSV(true))
                .WriteFormat(new WriteTSV(false))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\CSVToTSVComplex*.txt"))
                .Exec());
        }

        [Fact]
        public void CSVToParquetSimple()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\CSVSimple*.csv"))
                .ReadFormat(new ReadCSV(true))
                .WriteFormat(new WriteParquet())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\CSVToParquetSimple*.parquet"))
                .Exec());
        }

        [Fact]
        public void CSVToParquetComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\CSVComplex*.csv"))
                .ReadFormat(new ReadCSV(true))
                .WriteFormat(new WriteParquet())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\CSVToParquetComplex*.parquet"))
                .Exec());
        }

        [Fact]
        public void CSVToParquetUncompressed()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\CSVComplex*.csv"))
                .ReadFormat(new ReadCSV(true))
                .WriteFormat(new WriteParquet(false))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\CSVToParquetUncompressed*.parquet"))
                .Exec());
        }

        [Fact]
        public void JSONToTSVSimple()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\JSONSimple*.json"))
                .ReadFormat(new ReadJSON())
                .WriteFormat(new WriteTSV(false))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\JSONToTSVSimple*.txt"))
                .Exec());
        }

        [Fact]
        public void JSONToTSVComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\JSONComplex*.json"))
                .ReadFormat(new ReadJSON())
                .WriteFormat(new WriteTSV(false))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\JSONToTSVComplex*.txt"))
                .Exec());
        }
    }
}