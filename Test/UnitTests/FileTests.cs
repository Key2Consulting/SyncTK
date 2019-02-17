using SyncTK;
using System;
using Xunit;

namespace SyncTK.Test.UnitTests
{
    public class FileTests : TestBase
    {
        [Fact]
        public void TSVToParquetSimple()
        {
            new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVSimple*.txt"))
                .ReadFormat(new ReadTSV(true))
                .WriteFormat(new WriteParquet())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVToParquetSimple*.parquet"))
                .Exec();
        }

        [Fact]
        public void TSVToParquetComplex()
        {
            new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVComplex*.txt"))
                .ReadFormat(new ReadTSV(true))
                .WriteFormat(new WriteParquet())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVToParquetComplex*.parquet"))
                .Exec();
        }

        [Fact]
        public void ParquetToTSVSimple()
        {
            new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetSimple*.parquet"))
                .ReadFormat(new ReadParquet())
                .WriteFormat(new WriteTSV())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\ParquetToTSVSimple*.txt"))
                .Exec();
        }

        [Fact]
        public void ParquetToTSVComplex()
        {
            new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetSimple*.parquet"))
                .ReadFormat(new ReadParquet())
                .WriteFormat(new WriteParquet())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\ParquetToTSVSimple*.txt"))
                .Exec();
        }


        [Fact]
        public void TSVToParquetUncompressed()
        {
            new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVComplex*.txt"))
                .ReadFormat(new ReadTSV(true))
                .WriteFormat(new WriteParquet(false))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVToParquetUncompressed*.parquet"))
                .Exec();
        }
    }
}