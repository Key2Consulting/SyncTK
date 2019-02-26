using SyncTK;
using System;
using Xunit;
using Xunit.Abstractions;

namespace SyncTK.Test.UnitTests
{
    public class SqlServerTests : TestBase
    {
        public SqlServerTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TSVToDBSimple()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVSimple*.txt"))
                .ReadFormat(new ReadTSV())
                .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "TSVToDBSimple", true))
                .Exec());
        }

        [Fact]
        public void TSVToDBComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVComplex*.txt"))
                .ReadFormat(new ReadTSV())
                .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "TSVToDBComplex", true))
                .Exec());
        }

        [Fact]
        public void ParquetToDBSimple()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetSimple*.parquet"))
                .ReadFormat(new ReadParquet())
                .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "ParquetToDBSimple", true))
                .Exec());
        }

        [Fact]
        public void ParquetToDBComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetComplex*.parquet"))
                .ReadFormat(new ReadParquet())
                .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "ParquetToDBComplex", true))
                .Exec());
        }

        [Fact]
        public void CSVToDBSimple()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\CSVSimple*.csv"))
                .ReadFormat(new ReadCSV())
                .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "CSVToDBSimple", true))
                .Exec());
        }

        [Fact]
        public void CSVToDBComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\CSVComplex*.csv"))
                .ReadFormat(new ReadCSV())
                .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "CSVToDBComplex", true))
                .Exec());
        }

        [Fact]
        public void JSONToDBSimple()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\JSONSimple*.json"))
                .ReadFormat(new ReadJSON())
                .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "JSONToDBSimple", true))
                .Exec());
        }

        [Fact]
        public void JSONToDBComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\JSONComplex*.json"))
                .ReadFormat(new ReadJSON())
                .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "JSONToDBComplex", true))
                .Exec());
        }

        [Fact]
        public void DBToDBSimple()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", GetResource("SqlServerSimple.sql")))
                .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "DBToDBSimple", true))
                .Exec());
        }

        [Fact]
        public void DBToDBComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", GetResource("SqlServerComplex.sql")))
                .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "DBToDBComplex", true))
                .Exec());
        }

        [Fact]
        public void DBToJSONComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", GetResource("SqlServerComplex.sql")))
                .WriteFormat(new WriteJSON())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\DBToJSONComplex*.json"))
                .Exec());
        }
    }
}