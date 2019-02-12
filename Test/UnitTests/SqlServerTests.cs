using SyncTK;
using System;
using Xunit;

namespace SyncTK.Test.UnitTests
{
    public class SqlServerTests : TestBase
    {
        [Fact]
        public void DBToTSVLocal()
        {
            new Sync()
                .From(new SourceSqlServer(@"(LocalDb)\MSSQLLocalDB", "SyncTK", "sys", "objects"))
                .ConvertTo(new ConvertTSV(true))
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\DBToTSVLocal_*.txt"))
                .Exec();
        }

        [Fact]
        public void TSVToDB()
        {
            new Sync()
                .From(new SourceFile($"{GetConfig("SampleFilesRoot")}\\Sample10000.txt"))
                .WithFormat(new FormatTSV(true))
                .Into(new TargetSqlServer(@"(LocalDb)\MSSQLLocalDB", "SyncTK", "dbo", "TSVToDB", true))
                .Exec();
        }

        [Fact]
        public void DBToDBSimple()
        {
            new Sync()
                .From(new SourceSqlServer(@"(LocalDb)\MSSQLLocalDB", "SyncTK", "sys", "objects"))
                .Into(new TargetSqlServer(@"(LocalDb)\MSSQLLocalDB", "SyncTK", "dbo", "DBToDBSimple", true))
                .Exec();
        }

        [Fact]
        public void DBToDBOddTypes()
        {
            new Sync()
                .From(new SourceSqlServer(@"(LocalDb)\MSSQLLocalDB", "SyncTK", "SELECT t.* FROM [dbo].[OddTypes] t CROSS APPLY sys.objects a CROSS APPLY sys.objects b"))
                .Into(new TargetSqlServer(@"(LocalDb)\MSSQLLocalDB", "SyncTK", "dbo", "DBToDBOddTypes", true))
                .Exec();
        }

        [Fact]
        public void DBToDBLargeTable()
        {
            new Sync()
                .From(new SourceSqlServer(@"(LocalDb)\MSSQLLocalDB", "SyncTK", "SELECT TOP 1000000 t.* FROM sys.objects t CROSS APPLY sys.objects a CROSS APPLY sys.objects b CROSS APPLY sys.objects c"))
                .Into(new TargetSqlServer(@"(LocalDb)\MSSQLLocalDB", "SyncTK", "dbo", "DBToDBLargeTable", true))
                .Exec();
        }

        [Fact]
        public void DBToParquetLarge()
        {
            new Sync()
                .From(new SourceSqlServer(@"(LocalDb)\MSSQLLocalDB", "SyncTK", "SELECT TOP 1000000 t.* FROM sys.objects t CROSS APPLY sys.objects a CROSS APPLY sys.objects b CROSS APPLY sys.objects c"))
                .ConvertTo(new ConvertParquet())
                .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\DBToParquetLarge_*.parquet"))
                .Exec();
        }
    }
}