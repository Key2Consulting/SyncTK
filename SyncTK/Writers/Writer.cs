using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SyncTK
{
    internal interface IDataWriter
    {
        bool Write(StreamWriter writer, int fileNumber);
    }
}