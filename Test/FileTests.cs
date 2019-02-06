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
                .Write(new TSVReaderComponent(true))
                .Target(new FileSourceComponent($"{this.TempFilesRoot}\\Test1.txt"))
                .Exec();
        }
    }
}