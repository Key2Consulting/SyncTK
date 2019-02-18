using SyncTK;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace SyncTK.Test.UnitTests
{
    public class AzureTests : TestBase
    {
        public AzureTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void DBToWASBParquetComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", GetResource("SqlServerComplex.sql")))
                .WriteFormat(new WriteParquet())
                .Into(new TargetWASB(GetConfig("AzureBlobConnectionString"), GetConfig("AzureBlobContainer"), "SyncTKTest\\v1\\DBToWASBParquetComplex*.parquet"))
                .Exec());
        }

        [Fact]
        public void DBToWASBTSVComplex()
        {
            WritePipelineOutput(new Pipeline()
                .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", GetResource("SqlServerComplex.sql")))
                .WriteFormat(new WriteTSV())
                .Into(new TargetWASB(GetConfig("AzureBlobConnectionString"), GetConfig("AzureBlobContainer"), "SyncTKTest\\v1\\DBToWASBTSVComplex*.txt"))
                .Exec());
        }

        [Fact]
        public void DBToWASBParquetComplexAsync()
        {
            var tasks = new List<Task>();

            for (int i = 0; i < 3; i++)
            {
                var task = new Pipeline()
                    .From(new SourceSqlServer(GetConfig("SQLServer"), "SyncTK", GetResource("SqlServerComplex.sql")))
                    .WriteFormat(new WriteParquet())
                    .Into(new TargetWASB(GetConfig("AzureBlobConnectionString"), GetConfig("AzureBlobContainer"), $"SyncTKTest\\v1\\DBToWASBParquetComplexAsync{i}*.parquet"))
                    .ExecAsync();
                tasks.Add(task);
                task.Start();
            }

            Task.WaitAll(tasks.ToArray());
        }
    }
}