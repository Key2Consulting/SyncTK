using SyncTK;
using System;

namespace Test
{
    public class FileTests : Test
    {
        public void Test1()
        {
            Sync
                .Source(new FileSourceComponent($"{this.SampleFilesRoot}\\*.txt"))
                .Read(new TSVReaderComponent(true))
                .Write(new TSVWriterComponent(true))
                .Target(new FileTargetComponent($"{this.TempFilesRoot}\\Test1_*.txt"))
                .Exec();
        }
    }
}