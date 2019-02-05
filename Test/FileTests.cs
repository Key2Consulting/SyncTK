using SyncTK;
using System;

namespace Test
{
    public class FileTests : Test
    {
        public void Test1()
        {
            var src = new FileConnector();
            var r = new TSVReader(src.Export($"{this.SampleFileRoot}\\*.txt"), false);
        }
    }
}