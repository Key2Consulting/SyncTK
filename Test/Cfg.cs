using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SyncTK.Test
{
    public class Cfg
    {
        static public string SampleFilesRoot
        {
            get
            {
                return Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), @".\..\Test\SampleFiles"));
            }
        }

        static public string TempFilesRoot
        {
            get
            {
                return Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), @".\..\Test\TempFiles"));
            }
        }
    }
}