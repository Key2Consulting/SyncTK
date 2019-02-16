using System;
using System.Collections.Generic;
using System.Text;

namespace SyncTK
{
    internal class TypeConversionMap
    {
        public ColumnSchema Source = new ColumnSchema();
        public ColumnSchema Target = new ColumnSchema();
        public bool TransportAsBinary;
    }
}
