﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SyncTK
{
    internal interface IDataWriter
    {
        void Write(StreamWriter writer);
    }
}