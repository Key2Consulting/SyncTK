using SyncTK;
using System;
using Xunit;

namespace SyncTK.Test.UnitTests
{
    public class GenerateSampleFiles : TestBase
    {
        int _multipleFileCount;

        public GenerateSampleFiles()
        {
            _multipleFileCount = int.Parse(GetConfig("MultipleFileCount"));
        }

        [Fact]
        public void GenerateTSVSimple()
        {
            for (int i = 0; i < _multipleFileCount; i++)
            {
                new Pipeline()
                    .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", GetResource("SqlServerSimple.sql")))
                    .WriteFormat(new WriteTSV(true))
                    .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVSimple{i}.txt"))
                    .Exec();
            }
        }

        [Fact]
        public void GenerateTSVComplex()
        {
            for (int i = 0; i < _multipleFileCount; i++)
            {
                new Pipeline()
                    .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", GetResource("SqlServerComplex.sql")))
                    .WriteFormat(new WriteTSV(true))
                    .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\TSVComplex{i}.txt"))
                    .Exec();
            }
        }

        [Fact]
        public void GenerateParquetSimple()
        {
            for (int i = 0; i < _multipleFileCount; i++)
            {
                new Pipeline()
                    .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", GetResource("SqlServerSimple.sql")))
                    .WriteFormat(new WriteParquet())
                    .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\ParquetSimple{i}.parquet"))
                    .Exec();
            }
        }

        [Fact]
        public void GenerateParquetComplex()
        {
            for (int i = 0; i < _multipleFileCount; i++)
            {
                new Pipeline()
                    .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", GetResource("SqlServerComplex.sql")))
                    .WriteFormat(new WriteParquet())
                    .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\ParquetComplex{i}.parquet"))
                    .Exec();
            }
        }
    }
}