using SyncTK;
using System;
using Xunit;
using Xunit.Abstractions;

namespace SyncTK.Test.UnitTests
{
    public class GenerateSampleFiles : TestBase
    {
        int _maxRowsPerFile;
        int _maxRowsPerRowGroup;

        public GenerateSampleFiles(ITestOutputHelper output) : base(output)
        {
            _maxRowsPerFile = _dataSetSize / _sampleFileCount + _sampleFileCount;
            _maxRowsPerRowGroup = _maxRowsPerFile / 3;
        }

        [Fact]
        public void GenerateTSVSimple()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", GetResource("SqlServerSimple.sql")))
                .WriteFormat(new WriteTSV(true, _maxRowsPerFile))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVSimple_*.txt"))
                .Exec());
        }

        [Fact]
        public void GenerateTSVComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", GetResource("SqlServerComplex.sql")))
                .WriteFormat(new WriteTSV(true, _maxRowsPerFile))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVComplex_*.txt"))
                .Exec());
        }

        [Fact]
        public void GenerateParquetSimple()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", GetResource("SqlServerSimple.sql")))
                .WriteFormat(new WriteParquet(true, _maxRowsPerRowGroup, _maxRowsPerFile))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\ParquetSimple_*.parquet"))
                .Exec());
        }

        [Fact]
        public void GenerateParquetComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", GetResource("SqlServerComplex.sql")))
                .WriteFormat(new WriteParquet(true, _maxRowsPerRowGroup, _maxRowsPerFile))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\ParquetComplex_*.parquet"))
                .Exec());
        }
    }
}