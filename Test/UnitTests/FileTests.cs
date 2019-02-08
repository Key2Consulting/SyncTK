using SyncTK;
using System;
using Xunit;

namespace SyncTK.Test
{
    public class FileTests
    {
        [Fact]
        public void TSVToTSVLocal()
        {
            Sync
                .From(new SourceFile($"{Cfg.SampleFilesRoot}\\*.txt"))
                .WithFormat(new FormatTSV(true))
                .ConvertTo(new ConvertTSV(true))
                .Into(new TargetFile($"{Cfg.TempFilesRoot}\\Test1_*.txt"))
                .Exec();

        }

        [Fact]
        public void CSVToCSVLocalAsync()
        {
            var t = Sync
                .From(new SourceFile($"{Cfg.SampleFilesRoot}\\*.txt"))
                .WithFormat(new FormatTSV(true))
                .ConvertTo(new ConvertTSV(true))
                .Into(new TargetFile($"{Cfg.TempFilesRoot}\\Test1_*.txt"))
                .ExecAsync();

            t.Start();
            t.Wait();
        }
    }
}