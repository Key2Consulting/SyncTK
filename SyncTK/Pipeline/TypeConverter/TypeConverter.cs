using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncTK.Internal
{
    /// <summary>
    /// A simple wrapper around another DataReader that provides type conversion between various source and target formats.
    /// Conversions are handled by the TypeConversionTable.Convert method.
    /// </summary>
    internal class TypeConverter : Component, IDataReader
    {
        protected Source _source;
        protected ReadFormatter _readFormatter;
        protected WriteFormatter _writeFormatter;
        protected Target _target;
        protected IDataReader _reader;
        internal TypeConversionTable TypeConversionTable;
        protected int _totalReadCount = 0;

        internal TypeConverter(Source source, ReadFormatter readFormatter, WriteFormatter writeFormatter, Target target)
        {
            _source = source;
            _readFormatter = readFormatter;
            _writeFormatter = writeFormatter;
            _target = target;
        }

        internal override IEnumerable<object> Process(Pipeline pipeline, IEnumerable<object> input)
        {
            // Get our input explicitly via the input parameter, or implicitly via the upstream component.
            if (input == null)
            {
                _reader = (IDataReader)GetUpstreamComponent(pipeline);
                CreateTypeConversionTable();
                yield return this;
            }
            else
            {
                foreach (var i in input)
                {
                    _reader = (IDataReader)i;
                    CreateTypeConversionTable();
                    yield return this;
                }
            }
        }

        /// <summary>
        /// Creates a new TypeConversionTable with some general defaults and then apply
        /// source/target specific configurations.
        /// </summary>
        protected void CreateTypeConversionTable()
        {
            TypeConversionTable = new TypeConversionTable();

            // Extract source schema information and apply to both source and target
            // as the preferred type map by default.
            var schemaTable = _reader.GetSchemaTable();
            int columnIndex = 0;
            foreach (DataRow row in schemaTable.Rows)
            {
                var map = new TypeConversionMap();
                map.Source.ColumnName = row["ColumnName"].ToString();
                map.Target.ColumnName = row["ColumnName"].ToString();
                map.Source.DataTypeName = row["DataTypeName"].ToString().ToUpper();
                map.Target.DataTypeName = row["DataTypeName"].ToString().ToUpper();
                map.Source.ColumnSize = Cast<int>(row["ColumnSize"]);
                map.Target.ColumnSize = Cast<int>(row["ColumnSize"]);
                map.Source.NumericPrecision = Cast<Int16>(row["NumericPrecision"]);
                map.Target.NumericPrecision = Cast<Int16>(row["NumericPrecision"]);
                map.Source.NumericScale = Cast<Int16>(row["NumericScale"]);
                map.Target.NumericScale = Cast<Int16>(row["NumericScale"]);
                map.Source.AllowNull = Cast<bool>(row["AllowDBNull"]);
                map.Target.AllowNull = Cast<bool>(row["AllowDBNull"]);
                map.TransportAsBinary = false;
                

                // If column names are missing, derive based on index.
                if (map.Source.ColumnName == "")
                    map.Source.ColumnName = $"Column{columnIndex}";
                if (map.Target.ColumnName == "")
                    map.Target.ColumnName = $"Column{columnIndex}";

                // If column uses special type, strip off namespace prefix and default to binary.
                if (map.Source.DataTypeName.Contains("."))
                {
                    var dataTypeParts = map.Source.DataTypeName.Split('.');
                    map.Source.DataTypeName = dataTypeParts[dataTypeParts.Length - 1];
                    map.Target.DataTypeName = map.Source.DataTypeName;
                    map.Source.DataType = typeof(object);
                    map.Target.DataType = typeof(object);
                }
                else
                {
                    map.Source.DataType = Type.GetType(row["DataType"].ToString());
                    map.Target.DataType = Type.GetType(row["DataType"].ToString());
                }

                Map(map, _source);
                Map(map, _readFormatter);
                Map(map, _writeFormatter);
                Map(map, _target);

                TypeConversionTable.Add(map);
                columnIndex++;
            }
        }

        protected void Map(TypeConversionMap map, Component component)
        {
            if (component != null)
            {
                var componentType = component.GetType();
                var method = typeof(TypeConverter).GetMethod($"Map{componentType.Name}", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                if (method != null)
                    method.Invoke(this, new[] { map });
            }
        }

        protected void MapSourceSqlServer(TypeConversionMap map)
        {
            // If a variable length and the size is "unlimited", force size to -1 which denotes unlimited.
            if ((map.Source.DataTypeName.Contains("CHAR") || map.Source.DataTypeName.Contains("BINARY")) && map.Source.ColumnSize > 8000)
            {
                map.Source.ColumnSize = -1;
                map.Target.ColumnSize = -1;
            }

            // SQL Server special types (Geography, Geometry) require special assembly. However, if these are
            // converted to BINARY the assembly isn't required and SQL will convert in target.
            switch (map.Source.DataTypeName)
            {
                case "GEOGRAPHY":
                case "GEOMETRY":
                case "HIERARCHYID":
                    map.TransportAsBinary = true;
                    break;
            }
        }

        protected void MapTargetSqlServer(TypeConversionMap map)
        {
            switch (map.Source.DataTypeName)
            {
                // .NET strings convert to NVARCHAR(MAX).
                case "STRING":
                    map.Target.DataTypeName = "NVARCHAR";
                    map.Target.ColumnSize = -1;
                    break;
                case "BYTE":
                    map.Target.DataTypeName = "TINYINT";
                    break;
                case "INT16":
                    map.Target.DataTypeName = "SMALLINT";
                    break;
                case "INT32":
                    map.Target.DataTypeName = "INT";
                    break;
                case "INT64":
                    map.Target.DataTypeName = "BIGINT";
                    break;
                case "DOUBLE":
                    map.Target.DataTypeName = "BIGINT";
                    break;
            }
        }

        protected void MapReadParquet(TypeConversionMap map)
        {
            switch (map.Source.DataTypeName)
            {
                case "DECIMAL":
                    map.Source.NumericPrecision = 38;
                    map.Source.NumericScale = 18;
                    map.Source.ColumnSize = 16;
                    break;
                default:
                    break;
            }
        }

        protected void MapWriteParquet(TypeConversionMap map)
        {
            // If we're required to transport the data as binary, we need to store it in Parquet as binary as well.
            if (map.TransportAsBinary)
            {
                map.Target.DataType = typeof(byte[]);
                map.Target.DataTypeName = "System.Byte[]";
            }

            // Specific type conversions.
            switch (map.Source.DataTypeName)
            {
                // Parquet defaults to using DateTimeOffset over DateTime.
                case "SMALLDATETIME":
                case "DATETIME2":
                case "DATETIME":
                    map.Target.DataType = typeof(DateTimeOffset);
                    map.Target.DataTypeName = "DateTimeOffset";
                    break;
                // Parquest doesn't support GUID, but that's easily converted to a string.
                case "UNIQUEIDENTIFIER":
                case "GUID":
                    map.Target.DataType = typeof(string);
                    map.Target.DataTypeName = "STRING";
                    break;
                case "TIME":
                    map.Target.DataType = typeof(DateTimeOffset);
                    map.Target.DataTypeName = "DateTimeOffset";
                    break;
                case "SQL_VARIANT":
                    map.Target.DataType = typeof(string);
                    map.Target.DataTypeName = "STRING";
                    break;
                default:
                    break;
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

        #region IDataReader Interface
        public object this[int i]
        {
            get
            {
                return _reader[i];
            }
        }

        public object this[string name]
        {
            get
            {
                return _reader[name];
            }
        }

        public int Depth
        {
            get
            {
                return _reader.Depth;
            }
        }

        public bool IsClosed
        {
            get
            {
                return _reader.IsClosed;
            }
        }

        public int RecordsAffected
        {
            get
            {
                return _reader.RecordsAffected;
            }
        }

        public int FieldCount
        {
            get
            {
                return _reader.FieldCount;
            }
        }

        public void Close()
        {
            _reader.Close();
        }

        public void Dispose()
        {
            _reader.Dispose();
        }

        public bool GetBoolean(int i)
        {
            return _reader.GetBoolean(i);
        }

        public byte GetByte(int i)
        {
            return _reader.GetByte(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return _reader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        }

        public char GetChar(int i)
        {
            return _reader.GetChar(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return _reader.GetChars(i, fieldoffset, buffer, bufferoffset, length);
        }

        public IDataReader GetData(int i)
        {
            return _reader.GetData(i);
        }

        public string GetDataTypeName(int i)
        {
            return _reader.GetDataTypeName(i);
        }

        public DateTime GetDateTime(int i)
        {
            return _reader.GetDateTime(i);
        }

        public decimal GetDecimal(int i)
        {
            return _reader.GetDecimal(i);
        }

        public double GetDouble(int i)
        {
            return _reader.GetDouble(i);
        }

        public Type GetFieldType(int i)
        {
            return _reader.GetFieldType(i);
        }

        public float GetFloat(int i)
        {
            return _reader.GetFloat(i);
        }

        public Guid GetGuid(int i)
        {
            return _reader.GetGuid(i);
        }

        public short GetInt16(int i)
        {
            return _reader.GetInt16(i);
        }

        public int GetInt32(int i)
        {
            return _reader.GetInt32(i);
        }

        public long GetInt64(int i)
        {
            return _reader.GetInt64(i);
        }

        public string GetName(int i)
        {
            return _reader.GetName(i);
        }

        public int GetOrdinal(string name)
        {
            return _reader.GetOrdinal(name);
        }

        public DataTable GetSchemaTable()
        {
            return _reader.GetSchemaTable();
        }

        public string GetString(int i)
        {
            return _reader.GetString(i);
        }

        /// <summary>
        /// Performs conversion for a value based on the conversion table.
        /// </summary>
        /// <param name="i">Index of the column containing the value.</param>
        /// <returns></returns>
        public object GetValue(int i)
        {
            // GetValue is called by SqlBulkCopy and where the real work takes place. Based on the 
            // TypeConversionMap, will return the appropriate type.
            var map = TypeConversionTable[i];
            if (map.Target.DataType == typeof(string))
            {
                return _reader.GetValue(i).ToString();
            }
            else if (map.Target.DataType == typeof(DateTimeOffset))
            {
                var val = _reader.GetValue(i);
                if (map.Source.DataType == typeof(DateTime))
                    return new DateTimeOffset((DateTime)_reader.GetValue(i));
                else if (map.Source.DataType == typeof(TimeSpan))
                    return DateTimeOffset.MinValue + (TimeSpan)_reader.GetValue(i);
                else
                    return Convert.ChangeType(_reader.GetValue(i), map.Target.DataType);
            }
            else if (map.TransportAsBinary)
            {
                var size = _reader.GetBytes(i, 0, null, 0, 0);
                var buffer = new byte[size];
                _reader.GetBytes(i, 0, buffer, 0, (int)size);
                return buffer;
            }
            else
            {
                if (_reader.IsDBNull(i))
                    return null;
                else
                {
                    return Convert.ChangeType(_reader.GetValue(i), map.Target.DataType);
                }
            }
        }

        public int GetValues(object[] values)
        {
            return _reader.GetValues(values);
        }

        public bool IsDBNull(int i)
        {
            return _reader.IsDBNull(i);
        }

        public bool NextResult()
        {
            return _reader.NextResult();
        }

        public bool Read()
        {
            if (_reader.Read())
            {
                _totalReadCount++;
                return true;
            }
            return false;
        }
        #endregion
    }
}