using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Test
{
    public class Test
    {
        public string SampleFileRoot
        {
            get
            {
                return Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), @".\..\..\..\SampleFiles"));
            }
        }
    }
}
