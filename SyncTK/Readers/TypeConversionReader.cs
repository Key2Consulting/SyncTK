using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Data.SqlClient;

namespace SyncTK
{
    /// <summary>
    /// A simple wrapper around another DataReader that provides type conversion according to a provided TypeConversionTable.
    /// All conversions are delegated to the TypeConversionTable.Convert method.
    /// </summary>
    internal class TypeConversionReader : System.Data.IDataReader
    {
        protected IDataReader _reader = null;
        protected TypeConversionTable _typeConversionTable;

        public TypeConversionReader(IDataReader reader, TypeConversionTable typeConversionTable)
        {
            _reader = reader;
            _typeConversionTable = typeConversionTable;
        }

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

        public object GetValue(int i)
        {
            // GetValue is called by SqlBulkCopy and where the real work takes place. Based on the 
            // TypeConversionMap, will return the appropriate type.
            return _typeConversionTable.ConvertValue(_reader, i);
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
            return _reader.Read();
        }
    }
}
