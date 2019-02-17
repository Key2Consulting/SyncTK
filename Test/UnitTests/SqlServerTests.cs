using SyncTK;
using System;
using Xunit;

namespace SyncTK.Test.UnitTests
{
    public class SqlServerTests : TestBase
    {
        [Fact]
        public void TSVToDBSimple()
        {
            new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVSimple*.txt"))
                .ReadFormat(new ReadTSV())
                .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "TSVToDBSimple", true))
                .Exec();
        }

        [Fact]
        public void TSVToDBComplex()
        {
            new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVComplex*.txt"))
                .ReadFormat(new ReadTSV())
                .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "TSVToDBComplex", true))
                .Exec();
        }

        [Fact]
        public void ParquetToDBSimple()
        {
            new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetSimple*.parquet"))
                .ReadFormat(new ReadParquet())
                .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "ParquetToDBSimple", true))
                .Exec();
        }

        [Fact]
        public void ParquetToDBComplex()
        {
            new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetComplex*.parquet"))
                .ReadFormat(new ReadTSV())
                .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "ParquetToDBComplex", true))
                .Exec();
        }

        [Fact]
        public void DBToDBSimple()
        {
            new Pipeline()
                .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", GetResource("SqlServerSimple.sql")))
                .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "DBToDBSimple", true))
                .Exec();
        }

        [Fact]
        public void DBToDBComplex()
        {
            new Pipeline()
                .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", GetResource("SqlServerComplex.sql")))
                .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "DBToDBComplex", true))
                .Exec();
        }
    }
}