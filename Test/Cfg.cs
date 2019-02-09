using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SyncTK.Test
{
    public class Cfg
    {
        static public string RootPath = Directory.GetCurrentDirectory();

        static public string SampleFilesRoot
        {
            get
            {
                return Path.GetFullPath(Path.Combine(RootPath, @".\..\..\..\SampleFiles"));
            }
        }

        static public string TempFilesRoot
        {
            get
            {
                return Path.GetFullPath(Path.Combine(RootPath, @".\..\..\..\TempFiles"));
            }
        }
    }
}