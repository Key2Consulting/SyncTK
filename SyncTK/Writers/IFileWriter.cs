using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SyncTK
{
    internal interface IFileWriter
    {
        bool Write(StreamWriter writer, int fileReadNumber, int fileWriteNumber);
    }
}