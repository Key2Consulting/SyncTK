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
                var map = new TypeConversionItem()
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

                // If column names are missing, derive based on index.
                if (map.SourceColumnName == "")
                    map.SourceColumnName = $"Column{columnIndex}";
                if (map.TargetColumnName == "")
                    map.TargetColumnName = $"Column{columnIndex}";

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

                Map(map, _source);
                Map(map, _readFormatter);
                Map(map, _writeFormatter);
                Map(map, _target);

                TypeConversionTable.Add(map);
                columnIndex++;
            }
        }

        protected void Map(TypeConversionItem map, Component component)
        {
            if (component != null)
            {
                var componentType = component.GetType();
                var method = typeof(TypeConverter).GetMethod($"Map{componentType.Name}", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                if (method != null)
                    method.Invoke(this, new[] { map });
            }
        }

        protected void MapSourceSqlServer(TypeConversionItem map)
        {
            // If a variable length and the size is "unlimited", force size to -1 which denotes unlimited.
            if ((map.SourceDataTypeName.Contains("CHAR") || map.SourceDataTypeName.Contains("BINARY")) && map.SourceColumnSize > 8000)
            {
                map.SourceColumnSize = -1;
                map.TargetColumnSize = -1;
            }

            // SQL Server special types (Geography, Geometry) require special assembly. However, if these are
            // converted to BINARY the assembly isn't required and SQL will convert in target.
            switch (map.SourceDataTypeName)
            {
                case "GEOGRAPHY":
                case "GEOMETRY":
                case "HIERARCHYID":
                    map.TransportAsBinary = true;
                    break;
            }
        }

        protected void MapTargetSqlServer(TypeConversionItem map)
        {
            switch (map.SourceDataTypeName)
            {
                // .NET strings convert to NVARCHAR(MAX).
                case "STRING":
                    map.TargetDataTypeName = "NVARCHAR";
                    map.TargetColumnSize = -1;
                    break;
            }
        }

        protected void MapWriteParquet(TypeConversionItem map)
        {
            // If we're required to transport the data as binary, we need to store it in Parquet as binary as well.
            if (map.TransportAsBinary)
            {
                map.TargetDataType = typeof(byte[]);
                map.TargetDataTypeName = "System.Byte[]";
            }

            // Specific type conversions.
            switch (map.SourceDataTypeName)
            {
                // Parquet defaults to using DateTimeOffset over DateTime.
                case "SMALLDATETIME":
                case "DATETIME2":
                case "DATETIME":
                    map.TargetDataType = typeof(DateTimeOffset);
                    map.TargetDataTypeName = "DateTimeOffset";
                    break;
                // Parquest doesn't support GUID, but that's easily converted to a string.
                case "UNIQUEIDENTIFIER":
                case "GUID":
                    map.TargetDataType = typeof(string);
                    map.TargetDataTypeName = "STRING";
                    break;
                case "TIME":
                    map.TargetDataType = typeof(DateTimeOffset);
                    map.TargetDataTypeName = "DateTimeOffset";
                    break;
                case "SQL_VARIANT":
                    map.TargetDataType = typeof(string);
                    map.TargetDataTypeName = "STRING";
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
            if (map.TargetDataType == typeof(string))
            {
                return _reader.GetValue(i).ToString();
            }
            else if (map.TargetDataType == typeof(DateTimeOffset))
            {
                var val = _reader.GetValue(i);
                if (map.SourceDataType == typeof(DateTime))
                    return new DateTimeOffset((DateTime)_reader.GetValue(i));
                else if (map.SourceDataType == typeof(TimeSpan))
                    return DateTimeOffset.MinValue + (TimeSpan)_reader.GetValue(i);
                else
                    return Convert.ChangeType(_reader.GetValue(i), map.TargetDataType);
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
                    return Convert.ChangeType(_reader.GetValue(i), map.TargetDataType);
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