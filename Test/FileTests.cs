using SyncTK;
using System;

namespace Test
{
    public class FileTests : Test
    {
        public void Test1()
        {
            Sync
                .Source(new FileConnector($"{this.SampleFileRoot}\\*.txt"))
                .Read(new TSVReader(true))
                .Write(new TSVWriter())
                .Target(new FileConnector($"{this.SampleFileRoot}\\*.txt"));
        }
    }
}