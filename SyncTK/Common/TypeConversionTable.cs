using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace SyncTK
{
    internal class TypeConversionTable : List<TypeConversionMap>
    {
        public TypeConversionTable(Type source, Type target, DataTable schemaTable)
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

                // If column uses special type, strip off namespace prefix.
                var dataTypeParts = map.SourceDataTypeName.Split('.');
                map.SourceDataTypeName = dataTypeParts[dataTypeParts.Length - 1];
                map.TargetDataTypeName = map.SourceDataTypeName;

                // Sql Server Source
                if (source == typeof(SourceSqlServer))
                {
                    if ((map.SourceDataTypeName.Contains("CHAR") || map.SourceDataTypeName.Contains("BINARY")) && map.SourceColumnSize > 8000)
                    {
                        map.SourceColumnSize = -1;
                        map.TargetColumnSize = -1;
                    }
                }

                // Sql Server Target
                if (target == typeof(TargetSqlServer))
                {
                    switch (map.SourceDataTypeName)
                    {
                        case "STRING":
                            map.TargetDataTypeName = "NVARCHAR";
                            map.TargetColumnSize = -1;
                            break;
                    }
                }

                // If a special type, force conversion compatible between different database platforms.
                //

                // If a special type, must transport as binary.
                switch (map.SourceDataTypeName)
                {
                    case "GEOGRAPHY":
                        map.TransportAsBinary = true;
                        break;
                    case "GEOMETRY":
                        map.TransportAsBinary = true;
                        break;
                    case "HIERARCHYID":
                        map.TransportAsBinary = true;
                        break;
                }

                // If a special type, and source and target are different systems and/or formats, save as binary.
                if (map.TransportAsBinary && source.BaseType != target.BaseType)
                {
                    map.TargetDataTypeName = "BINARY";
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
    }
}