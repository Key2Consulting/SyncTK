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
        public int ColumnSize;              // size of the data type or length of the value (-1 is unlimited)
        public Int16 NumericPrecision;
        public Int16 NumericScale;
        public bool AllowNull;

        public ColumnSchema Clone()
        {
            var clone = new ColumnSchema
            {
                ColumnName = this.ColumnName,
                DataType = this.DataType,
                DataTypeName = this.DataTypeName,
                ColumnSize = this.ColumnSize,
                NumericPrecision = this.NumericPrecision,
                NumericScale = this.NumericScale,
                AllowNull = this.AllowNull
            };

            return clone;
        }
    }
}
