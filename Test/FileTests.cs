using SyncTK;
using System;

namespace Test
{
    public class FileTests : Test
    {
        public void Test1()
        {
            Sync
                .From(new SourceFile($"{this.SampleFilesRoot}\\*.txt"))
                .WithFormat(new FormatTSV(true))
                .ConvertTo(new ConvertTSV(true))
                .Into(new TargetFile($"{this.TempFilesRoot}\\Test1_*.txt"))
                .Exec();
        }
    }
}