using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Data.SqlClient;

namespace SyncTK
{
    /// <summary>
    /// A wrapper around another DataReader that provides type conversion between source and target. Without this 
    /// wrapper class, certain runtime types (e.g. Sql Geography) would cause type errors. For most of those
    /// cases, simply retrieving the data as a binary byte stream addresses the issue. Must preserve special type
    /// when target is same as source.
    /// </summary>
    internal class TypeConversionReader : System.Data.IDataReader
    {
        IDataReader _reader = null;
        Type _source = null;
        Type _target = null;
        public TypeConversionTable ConversionTable;
        bool[] _transportAsBinary;               // if true, the given column requires byte streaming

        public TypeConversionReader(IDataReader reader, Type source)
        {
            _reader = reader;
            _source = source;
        }
        
        public void SetTarget(Type target)
        {
            _target = target;

            // Certain columns require special processing and type conversions. Inspect the schema table to determine
            // which columns require which processing, and build highly optimized lookups to perform this conversion
            // during read operations.
            ConversionTable = new TypeConversionTable(_source, _target, _reader.GetSchemaTable());

            _transportAsBinary = new bool[this.FieldCount];
            for (int i = 0; i < this.FieldCount; i++)
            {
                _transportAsBinary[i] = ConversionTable[i].TransportAsBinary;
            }
        }

        public object this[int i]
        {
            get
            {
                return this._reader[i];
            }
        }

        public object this[string name]
        {
            get
            {
                return this._reader[name];
            }
        }

        public int Depth
        {
            get
            {
                return this._reader.Depth;
            }
        }

        public bool IsClosed
        {
            get
            {
                return this._reader.IsClosed;
            }
        }

        public int RecordsAffected
        {
            get
            {
                return this._reader.RecordsAffected;
            }
        }

        public int FieldCount
        {
            get
            {
                return this._reader.FieldCount;
            }
        }

        public void Close()
        {
            this._reader.Close();
        }

        public void Dispose()
        {
            this._reader.Dispose();
        }

        public bool GetBoolean(int i)
        {
            return this._reader.GetBoolean(i);
        }

        public byte GetByte(int i)
        {
            return this._reader.GetByte(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return this._reader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        }

        public char GetChar(int i)
        {
            return this._reader.GetChar(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return this._reader.GetChars(i, fieldoffset, buffer, bufferoffset, length);
        }

        public IDataReader GetData(int i)
        {
            return this._reader.GetData(i);
        }

        public string GetDataTypeName(int i)
        {
            return this._reader.GetDataTypeName(i);
        }

        public DateTime GetDateTime(int i)
        {
            return this._reader.GetDateTime(i);
        }

        public decimal GetDecimal(int i)
        {
            return this._reader.GetDecimal(i);
        }

        public double GetDouble(int i)
        {
            return this._reader.GetDouble(i);
        }

        public Type GetFieldType(int i)
        {
            return this._reader.GetFieldType(i);
        }

        public float GetFloat(int i)
        {
            return this._reader.GetFloat(i);
        }

        public Guid GetGuid(int i)
        {
            return this._reader.GetGuid(i);
        }

        public short GetInt16(int i)
        {
            return this._reader.GetInt16(i);
        }

        public int GetInt32(int i)
        {
            return this._reader.GetInt32(i);
        }

        public long GetInt64(int i)
        {
            return this._reader.GetInt64(i);
        }

        public string GetName(int i)
        {
            return this._reader.GetName(i);
        }

        public int GetOrdinal(string name)
        {
            return this._reader.GetOrdinal(name);
        }

        public DataTable GetSchemaTable()
        {
            // For whatever reason, we haven't needed to pass our converted schema table. Perhaps byte streaming is enough.
            // return _schemaTable;
            return this._reader.GetSchemaTable();
        }

        public string GetString(int i)
        {
            return this._reader.GetString(i);
        }

        public object GetValue(int i)
        {
            if (this._transportAsBinary[i])
            {
                // Console.WriteLine("Transporting binary for col{0}", i);
                var size = this.GetBytes(i, 0, null, 0, 0);
                var buffer = new byte[size];
                this.GetBytes(i, 0, buffer, 0, (int)size);
                return buffer;
            }
            else
            {
                return this._reader.GetValue(i);
            }
        }

        public int GetValues(object[] values)
        {
            return this._reader.GetValues(values);
        }

        public bool IsDBNull(int i)
        {
            return this._reader.IsDBNull(i);
        }

        public bool NextResult()
        {
            return this._reader.NextResult();
        }

        public bool Read()
        {
            return this._reader.Read();
        }
    }
}
