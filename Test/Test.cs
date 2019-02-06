using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Test
{
    public class Test
    {
        public string SampleFilesRoot
        {
            get
            {
                return Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), @".\..\..\..\SampleFiles"));
            }
        }

        public string TempFilesRoot
        {
            get
            {
                return Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), @".\..\..\..\TempFiles"));
            }
        }
    }
}
