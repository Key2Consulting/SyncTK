using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncTK.Internal
{
    public abstract class ReadFormatter : Component, IDataReader
    {
        internal List<ColumnSchema> _columnSchema = new List<ColumnSchema>();
        protected DataTable _schemaTable;
        protected object [] _readBuffer;
        protected IEnumerator<object> _input;
        protected StreamReader _reader;
        protected int _totalReadCount = 0;

        internal ReadFormatter()
        {
        }

        internal override IEnumerable<object> Process(IEnumerable<object> input)
        {
            _input = input.GetEnumerator();

            // We've just started to get first stream and raise initialization events.
            if (_input.MoveNext())
            {
                _reader = (StreamReader)_input.Current;
                OnBeginReading();

                // Allocate read buffer.  It's assummed column schema is created during OnBeginReading.
                Assert(_columnSchema.Count > 0, "Column schema not defined prior to reading data.");
                if (_readBuffer == null)
                    _readBuffer = new object[_columnSchema.Count];

                OnBeginFile();
            }
            else
            {
                // It would be very odd to have no input streams when no records have been read.
            }

            return null;
        }

        protected virtual void OnBeginReading()
        {
        }

        protected virtual void OnBeginFile()
        {
        }

        protected virtual void OnEndReading()
        {
        }

        protected abstract bool OnReadLine();

        #region IDataReader Interface
        public object this[int i]
        {
            get
            {
                return _readBuffer[i];
            }
        }

        public object this[string name]
        {
            get
            {
                for (int i = 0; i < _columnSchema.Count; i++)
                {
                    if (_columnSchema[i].ColumnName == name)
                        return _readBuffer[i];
                }
                return null;
            }
        }

        public int Depth
        {
            get
            {
                return -1;
            }
        }

        public bool IsClosed
        {
            get
            {
                if (_reader != null || _reader.BaseStream.Position <= 0)
                    return true;
                else
                    return true;
            }
        }

        public int RecordsAffected
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public int FieldCount
        {
            get
            {
                return _columnSchema.Count;
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
            throw new NotImplementedException();
        }

        public byte GetByte(int i)
        {
            throw new NotImplementedException();
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            throw new NotImplementedException();
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            // We only support string types, which will get converted to VARCHAR (MAX) or similar type depending on the 
            // target platform. That's up to the importer to determine.
            return "string";
        }

        public DateTime GetDateTime(int i)
        {
            throw new NotImplementedException();
        }

        public decimal GetDecimal(int i)
        {
            throw new NotImplementedException();
        }

        public double GetDouble(int i)
        {
            throw new NotImplementedException();
        }

        public Type GetFieldType(int i)
        {
            return typeof(string);
        }

        public float GetFloat(int i)
        {
            throw new NotImplementedException();
        }

        public Guid GetGuid(int i)
        {
            throw new NotImplementedException();
        }

        public short GetInt16(int i)
        {
            throw new NotImplementedException();
        }

        public int GetInt32(int i)
        {
            throw new NotImplementedException();
        }

        public long GetInt64(int i)
        {
            throw new NotImplementedException();
        }

        public string GetName(int i)
        {
            return _columnSchema[i].ColumnName;
        }

        public int GetOrdinal(string name)
        {
            for (int i = 0; i < _columnSchema.Count; i++)
            {
                if (_columnSchema[i].ColumnName == name)
                    return i;
            }
            return -1;
        }

        public string GetString(int i)
        {
            return (string)_readBuffer[i];
        }

        public object GetValue(int i)
        {
            return _readBuffer[i];
        }

        public int GetValues(object[] values)
        {
            values = _readBuffer;
            return _readBuffer.Length;
        }

        public bool IsDBNull(int i)
        {
            return (_readBuffer[i] == null);
        }

        public bool NextResult()
        {
            throw new NotImplementedException();
        }

        public bool Read()
        {
            try
            {
                // Read next line.
                bool read = OnReadLine();

                // If we're out of data in the current stream.
                if (!read)
                {
                    _reader.Dispose();

                    // Attempt to get a new stream from our source.
                    if (!_input.MoveNext())
                    {
                        // We're out of data, clean up and inform our consumer.
                        OnEndReading();
                        return false;
                    }

                    // Still have input streams to process.
                    _reader = (StreamReader)_input.Current;
                    OnBeginFile();
                    read = OnReadLine();
                    if (!read)
                        return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                throw new Exception($"Error in TSVReader reading line {_totalReadCount}.", ex);
            }
        }

        public DataTable GetSchemaTable()
        {
            if (_schemaTable == null)
            {
                _schemaTable = new DataTable();
                _schemaTable.Clear();
                _schemaTable.Columns.Add("ColumnName");
                _schemaTable.Columns.Add("ColumnOrdinal");
                _schemaTable.Columns.Add("ColumnSize");
                _schemaTable.Columns.Add("DataType");
                _schemaTable.Columns.Add("DataTypeName");
                _schemaTable.Columns.Add("AllowDBNull");
                _schemaTable.Columns.Add("NumericPrecision");
                _schemaTable.Columns.Add("NumericScale");

                // For each column in the input text file
                for (int i = 0; i < _columnSchema.Count; i++)
                {
                    // Add a row describing that column's schema.
                    DataRow textCol = _schemaTable.NewRow();
                    textCol["ColumnName"] = _columnSchema[i].ColumnName;
                    textCol["ColumnOrdinal"] = i;
                    textCol["ColumnSize"] = _columnSchema[i].ColumnSize;
                    textCol["DataType"] = _columnSchema[i].DataType;
                    textCol["DataTypeName"] = _columnSchema[i].DataTypeName;
                    textCol["AllowDBNull"] = _columnSchema[i].AllowNull;
                    textCol["NumericPrecision"] = _columnSchema[i].NumericPrecision;
                    textCol["NumericScale"] = _columnSchema[i].NumericScale;
                    _schemaTable.Rows.Add(textCol);
                }
            }

            return _schemaTable;
        }
        #endregion
    }
}