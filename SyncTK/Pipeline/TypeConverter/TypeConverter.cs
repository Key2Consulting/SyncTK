using Newtonsoft.Json;
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
    /// <remarks>
    /// The premise of type conversion is that sources should translate their types to a standard type and targets should
    /// translate from the standard type to their type. During translation, type information can get lost, so there are exceptions
    /// to the rule. For instance, when copying between the same source and/or format, the target can elect to accept the original
    /// source type information.
    /// </remarks>
    internal class TypeConverter : Component, IDataReader
    {
        protected Source _source;
        protected ReadFormatter _readFormatter;
        protected WriteFormatter _writeFormatter;
        protected Target _target;
        protected IDataReader _reader;
        internal TypeConversionTable TypeConversionTable;
        internal int _totalReadCount = 0;

        internal TypeConverter(Source source, ReadFormatter readFormatter, WriteFormatter writeFormatter, Target target)
        {
            _source = source;
            _readFormatter = readFormatter;
            _writeFormatter = writeFormatter;
            _target = target;
        }

        internal override IEnumerable<object> Process(IEnumerable<object> input)
        {
            // Get our input explicitly via the input parameter, or implicitly via the upstream component.
            if (input == null)
            {
                _reader = (IDataReader)GetUpstreamComponent();
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

        internal override void End()
        {
            _pipeline.AddLog("Rows", _totalReadCount);
            _pipeline.AddLog("TypeConversionTable", JsonConvert.SerializeObject(TypeConversionTable));
        }

        /// <summary>
        /// Creates a new TypeConversionTable with some general defaults and then apply
        /// source/target specific configurations.
        /// </summary>
        protected void CreateTypeConversionTable()
        {
            TypeConversionTable = new TypeConversionTable();

            // Extract source schema information.
            var schemaTable = _reader.GetSchemaTable();
            int columnIndex = 0;
            foreach (DataRow row in schemaTable.Rows)
            {
                // Default each source column's conversion to the data reader's schema table.
                var map = new TypeConversionMap();
                map.Source.ColumnName = row["ColumnName"].ToString();
                map.Source.DataType = Type.GetType(row["DataType"].ToString());
                map.Source.DataTypeName = row["DataTypeName"].ToString();
                map.Source.ColumnSize = Cast<int>(row["ColumnSize"]);
                map.Source.NumericPrecision = Cast<Int16>(row["NumericPrecision"]);
                map.Source.NumericScale = Cast<Int16>(row["NumericScale"]);
                map.Source.AllowNull = Cast<bool>(row["AllowDBNull"]);
                map.TransportAsBinary = false;

                // If column names are missing, derive based on index.
                if (map.Source.ColumnName == "")
                    map.Source.ColumnName = $"Column{columnIndex}";

                map.Source.DataTypeName = StripNamespace(map.Source.DataTypeName);

                // Default target to source (special conversion rules for target apply next).
                map.Target = map.Source.Clone();

                // Allow source, formatters, and target to apply their conversion (in that order).
                Map(map, _source);
                Map(map, _readFormatter);
                Map(map, _writeFormatter);
                Map(map, _target);

                TypeConversionTable.Add(map);
                columnIndex++;
            }
        }

        protected string StripNamespace(string typeName)
        {
            // If column uses a custom type, strip off namespace prefix and default to binary. This case occurs
            // when types use assemblies as their type information.
            if (typeName.Contains("."))
            {
                var dataTypeParts = typeName.Split('.');
                return dataTypeParts[dataTypeParts.Length - 1];
            }
            else
                return typeName;
        }

        protected string StripNamespace(Type type)
        {
            return StripNamespace(type.ToString());
        }

        protected void Map(TypeConversionMap map, Component component)
        {
            // Use reflection to invoke the concrete component's version of it's mapping logic (if exists).
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
            if ((map.Source.DataTypeName.ToUpper().Contains("CHAR") || map.Source.DataTypeName.ToUpper().Contains("BINARY")) && map.Source.ColumnSize > 8000)
            {
                map.Source.ColumnSize = -1;
                map.Target.ColumnSize = -1;
            }

            switch (map.Source.DataTypeName.ToUpper())
            {
                case "TINYINT":
                    map.Target.DataType = typeof(byte);
                    break;
                case "SMALLINT":
                    map.Target.DataType = typeof(Int16);
                    break;
                case "INT":
                    map.Target.DataType = typeof(Int32);
                    break;
                case "BIGINT":
                    map.Target.DataType = typeof(Int64);
                    break;
                case "SMALLDATETIME":
                case "DATETIME2":
                case "DATETIME":
                    map.Target.DataType = typeof(DateTimeOffset);
                    break;
                case "UNIQUEIDENTIFIER":
                    map.Target.DataType = typeof(string);
                    break;
                case "TIME":
                    map.Target.DataType = typeof(TimeSpan);
                    break;
                case "SQL_VARIANT":
                    map.Target.DataType = typeof(string);
                    break;
                case "REAL":
                    map.Target.DataType = typeof(double);
                    break;
                case "MONEY":
                case "SMALLMONEY":
                    map.Target.DataType = typeof(float);
                    break;
                case "NUMERIC":
                    map.Target.DataType = typeof(double);
                    break;
                // SQL Server special types (Geography, Geometry) require special assembly. However, if these are
                // converted to BINARY the assembly isn't required and SQL will convert in target.
                case "GEOGRAPHY":
                case "GEOMETRY":
                case "HIERARCHYID":
                    map.Target.DataType = typeof(byte[]);
                    map.TransportAsBinary = true;
                    break;
            }

            map.Target.DataTypeName = StripNamespace(map.Target.DataType);      // derive type name off actual type.
        }

        protected void MapTargetSqlServer(TypeConversionMap map)
        {
            // If the source is also SQL Server, use original source type information as target.
            if (_source is SourceSqlServer)
            {
                map.Target = map.Source;
            }
            else
            {
                switch (map.Target.DataTypeName.ToUpper())
                {
                    // .NET strings convert to NVARCHAR(MAX).
                    case "STRING":
                        map.Target.DataTypeName = "NVARCHAR";
                        map.Target.ColumnSize = -1;
                        break;
                    case "BYTE":
                        map.Target.DataTypeName = "TINYINT";
                        break;
                    case "SHORT":
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
                        map.Target.DataTypeName = "DECIMAL";
                        break;
                    case "SINGLE":
                        map.Target.DataTypeName = "FLOAT";
                        break;
                    case "BOOLEAN":
                        map.Target.DataTypeName = "BIT";
                        break;
                    case "BYTE[]":
                        map.Target.DataTypeName = "VARBINARY";
                        break;
                    default:
                        break;
                }

            }
        }

        protected void MapReadParquet(TypeConversionMap map)
        {
            switch (map.Source.DataTypeName.ToUpper())
            {
                case ("DATETIMEOFFSET"):
                    map.Target.DataType = typeof(DateTimeOffset);
                    break;
                case "DECIMAL":
                    map.Target.DataType = typeof(decimal);
                    map.Target.ColumnSize = 16;
                    map.Target.NumericPrecision = 38;
                    map.Target.NumericScale = 18;
                    break;
                case "DOUBLE":
                    map.Target.DataType = typeof(double);
                    map.Target.ColumnSize = sizeof(double);
                    map.Target.NumericPrecision = 38;
                    map.Target.NumericScale = 18;
                    break;
                case "SINGLE":
                case "FLOAT":
                    map.Target.DataType = typeof(float);
                    map.Target.ColumnSize = sizeof(float);
                    break;
                case "STRING":
                    map.Target.DataType = typeof(string);
                    break;
                case "UNSIGNEDSHORT":
                    map.Target.DataType = typeof(ushort);
                    map.Target.ColumnSize = sizeof(ushort);
                    break;
                case "SHORT":
                case "INT16":
                    map.Target.DataType = typeof(Int16);
                    map.Target.ColumnSize = sizeof(Int16);
                    break;
                case "INT32":
                    map.Target.DataType = typeof(Int32);
                    map.Target.ColumnSize = sizeof(Int32);
                    break;
                case "INT64":
                    map.Target.DataType = typeof(Int64);
                    map.Target.ColumnSize = sizeof(Int64);
                    break;
                case "BOOLEAN":
                    map.Target.DataType = typeof(bool);
                    map.Target.ColumnSize = sizeof(bool);
                    break;
                case "BYTEARRAY":
                    map.Target.DataType = typeof(byte[]);
                    break;
                default:
                    break;
            }

            map.Target.DataTypeName = StripNamespace(map.Target.DataType);      // derive type name off actual type.
        }

        protected void MapWriteParquet(TypeConversionMap map)
        {
            // If we're required to transport the data as binary, we need to store it in Parquet as binary as well.
            if (map.TransportAsBinary)
            {
                map.Target.DataType = typeof(byte[]);
            }

            map.Target.DataTypeName = StripNamespace(map.Target.DataType);      // derive type name off actual type.

            switch (map.Target.DataTypeName.ToUpper())
            {
                case "SBYTE":
                    map.Target.DataTypeName = "SignedByte";
                    map.Target.DataType = typeof(sbyte);
                    break;
                case "USHORT":
                    map.Target.DataTypeName = "UnsignedShort";
                    map.Target.DataType = typeof(ushort);
                    break;
                case "BYTE":        // Parquet.NET's UnsignedByte throws runtime error, and their Byte is signed, so upconvert to short
                case "INT16":
                case "SHORT":
                    map.Target.DataTypeName = "Short";
                    map.Target.DataType = typeof(short);
                    break;
                case "FLOAT":
                case "SINGLE":
                    map.Target.DataTypeName = "Float";
                    map.Target.DataType = typeof(float);
                    break;
                case "BYTE[]":
                    map.Target.DataTypeName = "ByteArray";
                    map.Target.DataType = typeof(byte[]);
                    break;
                case "TIMESPAN":
                    map.Target.DataType = typeof(DateTimeOffset);
                    map.Target.DataTypeName = "DateTimeOffset";
                    break;
                default:
                    break;
            }

            // Specific type conversions.
            //switch (map.Source.DataTypeName)
            //{
            //    // Parquet defaults to using DateTimeOffset over DateTime.
            //    case "SMALLDATETIME":
            //    case "DATETIME2":
            //    case "DATETIME":
            //        map.Target.DataType = typeof(DateTimeOffset);
            //        map.Target.DataTypeName = "DateTimeOffset";
            //        break;
            //    // Parquest doesn't support GUID, but that's easily converted to a string.
            //    case "UNIQUEIDENTIFIER":
            //    case "GUID":
            //        map.Target.DataType = typeof(string);
            //        map.Target.DataTypeName = "STRING";
            //        break;
            //    case "TIME":
            //        map.Target.DataType = typeof(DateTimeOffset);
            //        map.Target.DataTypeName = "DateTimeOffset";
            //        break;
            //    case "SQL_VARIANT":
            //        map.Target.DataType = typeof(string);
            //        map.Target.DataTypeName = "STRING";
            //        break;
            //    default:
            //        break;
            //}
        }

        protected void MapReadJSON(TypeConversionMap map)
        {
            switch (map.Source.DataTypeName.ToUpper())
            {
                case "DOUBLE":
                    map.Target.DataType = typeof(double);
                    map.Target.ColumnSize = sizeof(double);
                    map.Target.NumericPrecision = 38;
                    map.Target.NumericScale = 18;
                    break;
                case "STRING":
                case "ARRAYLIST":
                    map.Target.DataType = typeof(string);
                    break;
                case "INT16":
                    map.Target.DataType = typeof(Int16);
                    map.Target.ColumnSize = sizeof(Int16);
                    break;
                case "INT32":
                    map.Target.DataType = typeof(Int32);
                    map.Target.ColumnSize = sizeof(Int32);
                    break;
                case "INT64":
                    map.Target.DataType = typeof(Int64);
                    map.Target.ColumnSize = sizeof(Int64);
                    break;
                case "DATETIME":
                    map.Target.DataType = typeof(DateTime);
                    break;
                case "BOOLEAN":
                    map.Target.DataType = typeof(bool);
                    map.Target.ColumnSize = sizeof(bool);
                    break;
                default:
                    break;
            }

            map.Target.DataTypeName = StripNamespace(map.Target.DataType);      // derive type name off actual type.
        }

        protected void MapWriteJSON(TypeConversionMap map)
        {
            map.Target.DataTypeName = StripNamespace(map.Target.DataType);      // derive type name off actual type.

            switch (map.Target.DataTypeName.ToUpper())
            {
                case "SBYTE":
                case "USHORT":
                case "BYTE":
                case "INT16":
                case "SHORT":
                    map.Target.DataTypeName = "Short";
                    map.Target.DataType = typeof(short);
                    break;
                case "FLOAT":
                case "SINGLE":
                    map.Target.DataTypeName = "Float";
                    map.Target.DataType = typeof(float);
                    break;
                case "BYTE[]":
                    map.Target.DataTypeName = "String";
                    map.Target.DataType = typeof(string);
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