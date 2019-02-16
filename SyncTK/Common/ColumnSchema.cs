using System;
using System.Collections.Generic;
using System.Text;

namespace SyncTK
{
    internal class ColumnSchema
    {
        public string ColumnName;
        public string DataTypeName;
        public Type DataType;
        public int ColumnSize;
        public Int16 NumericPrecision;
        public Int16 NumericScale;
        public bool AllowNull;
    }
}
