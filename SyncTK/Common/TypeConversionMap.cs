using System;
using System.Collections.Generic;
using System.Text;

namespace SyncTK
{
    internal class TypeConversionMap
    {
        public string SourceColumnName;
        public string TargetColumnName;
        public string SourceDataTypeName;
        public string TargetDataTypeName;
        public int SourceColumnSize;
        public int TargetColumnSize;
        public Int16 SourceNumericPrecision;
        public Int16 TargetNumericPrecision;
        public Int16 SourceNumericScale;
        public Int16 TargetNumericScale;
        public bool SourceAllowNull;
        public bool TargetAllowNull;
        public bool TransportAsBinary;
    }
}
