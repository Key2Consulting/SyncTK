using SyncTK;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace SyncTK.Test.UnitTests
{
    public class ScaleTests : TestBase
    {
        int _maxRowsPerFile;
        int _maxRowsPerRowGroup;

        public ScaleTests()
        {
            _maxRowsPerFile = _dataSetSize / 5;                     // force 5 files to be created each test
            _maxRowsPerRowGroup = _maxRowsPerFile / 5;              // force 5 rowgroups per file (where applicable)
        }

        [Fact]
        public void AllSampleFilesMultiParquetAsync()
        {
            var tasks = new List<Task>
            {
                new Pipeline()
                    .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetSimple*.parquet"))
                    .ReadFormat(new ReadParquet())
                    .WriteFormat(new WriteParquet(true, _maxRowsPerRowGroup, _maxRowsPerFile))
                    .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\AllSampleFilesMultiParquetAsync_ParquetSimple_*.parquet"))
                    .ExecAsync(),

                new Pipeline()
                    .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetComplex*.parquet"))
                    .ReadFormat(new ReadParquet())
                    .WriteFormat(new WriteParquet(true, _maxRowsPerRowGroup, _maxRowsPerFile))
                    .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\AllSampleFilesMultiParquetAsync_ParquetComplex_*.parquet"))
                    .ExecAsync(),

                new Pipeline()
                    .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVSimple*.txt"))
                    .ReadFormat(new ReadTSV())
                    .WriteFormat(new WriteParquet(true, _maxRowsPerRowGroup, _maxRowsPerFile))
                    .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\AllSampleFilesMultiParquetAsync_TSVSimple_*.parquet"))
                    .ExecAsync(),

                new Pipeline()
                    .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVComplex*.txt"))
                    .ReadFormat(new ReadTSV())
                    .WriteFormat(new WriteParquet(true, _maxRowsPerRowGroup, _maxRowsPerFile))
                    .Into(new TargetFile($"{GetConfig("TempFilesRoot")}\\AllSampleFilesMultiParquetAsync_TSVComplex_*.parquet"))
                    .ExecAsync()
            };

            foreach (var task in tasks)
            {
                task.Start();
            }

            Task.WaitAll(tasks.ToArray());
        }

        [Fact]
        public void AllSampleFilesDBAsync()
        {
            var tasks = new List<Task>
            {
                new Pipeline()
                    .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetSimple*.parquet"))
                    .ReadFormat(new ReadParquet())
                    .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "AllSampleFilesDBAsync_ParquetSimple", true))
                    .ExecAsync(),

                new Pipeline()
                    .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetComplex*.parquet"))
                    .ReadFormat(new ReadParquet())
                    .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "AllSampleFilesDBAsync_ParquetComplex", true))
                    .ExecAsync(),

                new Pipeline()
                    .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVSimple*.txt"))
                    .ReadFormat(new ReadTSV())
                    .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "AllSampleFilesDBAsync_TSVSimple", true))
                    .ExecAsync(),

                new Pipeline()
                    .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\TSVComplex*.txt"))
                    .ReadFormat(new ReadTSV())
                    .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "AllSampleFilesDBAsync_TSVComplex", true))
                    .ExecAsync()
            };

            foreach (var task in tasks)
            {
                task.Start();
            }

            Task.WaitAll(tasks.ToArray());
        }

        [Fact]
        public void ParquetComplexParallelDBAsync()
        {
            var tasks = new List<Task>();

            for (int i = 0; i < _sampleFileCount; i++)
            {
                tasks.Add(new Pipeline()
                    .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetComplex*_{i}.parquet"))
                    .ReadFormat(new ReadParquet())
                    .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "ParquetComplexParallelDBAsync_ParquetComplex", true))
                    .ExecAsync());
            }

            foreach (var task in tasks)
            {
                task.Start();
            }

            Task.WaitAll(tasks.ToArray());
        }

        [Fact]
        public void ParquetComplexDBAsync()
        {
            new Pipeline()
                .From(new SourceFile($"{GetConfig("TempFilesRoot")}\\ParquetComplex*.parquet"))
                .ReadFormat(new ReadParquet())
                .Into(new TargetSqlServer(GetConfig("SQLServer"), "SyncTK", "dbo", "ParquetComplexDBAsync_ParquetComplex", true))
                .Exec();
        }
    }
}