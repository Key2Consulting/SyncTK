using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace SyncTK
{
    internal class TypeConversionTable : List<TypeConversionMap>
    {
        /// <summary>
        /// Creates a new TypeConversionTable with some general defaults. Each target should customize these
        /// defaults per their own type conversion requirements.
        /// </summary>
        /// <param name="schemaTable">DataTable from source IDataReader.GetSchemaTable()</param>
        public TypeConversionTable(DataTable schemaTable)
        {
            // Extract source schema information and apply to both source and target
            // as the preferred type map by default.
            foreach (DataRow row in schemaTable.Rows)
            {
                var map = new TypeConversionMap()
                {
                    SourceColumnName = row["ColumnName"].ToString(),
                    TargetColumnName = row["ColumnName"].ToString(),
                    SourceDataTypeName = row["DataTypeName"].ToString().ToUpper(),
                    TargetDataTypeName = row["DataTypeName"].ToString().ToUpper(),
                    SourceColumnSize = Cast<int>(row["ColumnSize"]),
                    TargetColumnSize = Cast<int>(row["ColumnSize"]),
                    SourceNumericPrecision = Cast<Int16>(row["NumericPrecision"]),
                    TargetNumericPrecision = Cast<Int16>(row["NumericPrecision"]),
                    SourceNumericScale = Cast<Int16>(row["NumericScale"]),
                    TargetNumericScale = Cast<Int16>(row["NumericScale"]),
                    SourceAllowNull = Cast<bool>(row["AllowDBNull"]),
                    TargetAllowNull = Cast<bool>(row["AllowDBNull"]),
                    TransportAsBinary = false
                };

                // If column uses special type, strip off namespace prefix and default to binary.
                if (map.SourceDataTypeName.Contains("."))
                {
                    var dataTypeParts = map.SourceDataTypeName.Split('.');
                    map.SourceDataTypeName = dataTypeParts[dataTypeParts.Length - 1];
                    map.TargetDataTypeName = map.SourceDataTypeName;
                    map.SourceDataType = typeof(object);
                    map.TargetDataType = typeof(object);
                }
                else
                {
                    map.SourceDataType = Type.GetType(row["DataType"].ToString());
                    map.TargetDataType = Type.GetType(row["DataType"].ToString());
                }

                // If a variable length and the size is "unlimited", force size to -1 which denotes unlimited.
                if ((map.SourceDataTypeName.Contains("CHAR") || map.SourceDataTypeName.Contains("BINARY")) && map.SourceColumnSize > 8000)
                {
                    map.SourceColumnSize = -1;
                    map.TargetColumnSize = -1;
                }

                this.Add(map);
            }
        }

        protected T Cast<T>(object value)
        {
            var str = value.ToString();

            if (typeof(T) == typeof(int))
            {
                if (str.Length == 0)
                    value = (int)0;
                else
                    value = int.Parse(str);
            }

            if (typeof(T) == typeof(Int16))
            {
                if (str.Length == 0)
                    value = (Int16)0;
                else
                    value = Int16.Parse(str);
            }

            if (typeof(T) == typeof(bool))
            {
                value = bool.Parse(str);
            }

            return (T)value;
        }
        
        /// <summary>
        /// Performs conversion for a value based on the current table.
        /// </summary>
        /// <param name="reader">IDataReader providing the value to convert.</param>
        /// <param name="columnIndex">Index of the column containing the value.</param>
        /// <returns></returns>
        public object ConvertValue(IDataReader reader, int columnIndex)
        {
            if (this[columnIndex].TransportAsBinary)
            {
                var size = reader.GetBytes(columnIndex, 0, null, 0, 0);
                var buffer = new byte[size];
                reader.GetBytes(columnIndex, 0, buffer, 0, (int)size);
                return buffer;
            }
            else if (this[columnIndex].SourceDataType == typeof(DateTime) && this[columnIndex].TargetDataType == typeof(DateTimeOffset))
            {
                return new DateTimeOffset((DateTime)reader.GetValue(columnIndex));
            }
            else
            {
                if (reader.IsDBNull(columnIndex))
                    return null;
                else
                {
                    return Convert.ChangeType(reader.GetValue(columnIndex), this[columnIndex].TargetDataType);
                    // return reader.GetValue(columnIndex);
                }
            }
        }
    }
}