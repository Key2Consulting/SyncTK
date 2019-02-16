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
        protected string [] _columnName;
        protected object [] _readBuffer;
        protected IEnumerator<object> _input;
        protected StreamReader _reader;
        protected int _readCount = 0;

        internal ReadFormatter()
        {
        }

        internal override IEnumerable<object> Process(Pipeline pipeline, IEnumerable<object> input)
        {
            _input = input.GetEnumerator();

            // We've just started to get first stream and raise initialization events.
            if (_input.MoveNext())
            {
                _reader = (StreamReader)_input.Current;
                OnBeginReading();
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
                for (int i = 0; i < _columnName.Length; i++)
                {
                    if (_columnName[i] == name)
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
                return _columnName.Length;
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
            return _columnName[i].ToString();
        }

        public int GetOrdinal(string name)
        {
            for (int i = 0; i < _columnName.Length; i++)
            {
                if (_columnName[i] == name)
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
            throw new NotImplementedException();
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
                throw new Exception($"Error in TSVReader reading line {_readCount}.", ex);
            }
        }

        public DataTable GetSchemaTable()
        {
            // Even though we only support the string data type, we must generate a SchemaTable
            // to adhere to the IDataReader standard (and required by importers).
            DataTable dt = new DataTable();
            dt.Clear();
            dt.Columns.Add("ColumnName");
            dt.Columns.Add("ColumnOrdinal");
            dt.Columns.Add("ColumnSize");
            dt.Columns.Add("DataType");
            dt.Columns.Add("DataTypeName");
            dt.Columns.Add("AllowDBNull");
            dt.Columns.Add("NumericPrecision");
            dt.Columns.Add("NumericScale");
            dt.Columns.Add("UdtAssemblyQualifiedName");

            // For each column in the input text file
            for (int i = 0; i < _columnName.Length; i++)
            {
                // Add a row describing that column's schema.
                DataRow textCol = dt.NewRow();
                textCol["ColumnName"] = _columnName[i];
                textCol["ColumnOrdinal"] = i;
                textCol["ColumnSize"] = -1;
                textCol["DataType"] = typeof(System.String);
                textCol["DataTypeName"] = "string";
                textCol["AllowDBNull"] = true;
                textCol["NumericPrecision"] = null;
                textCol["NumericScale"] = null;
                textCol["UdtAssemblyQualifiedName"] = "PowerSync.TextFileDataReader.String";
                dt.Rows.Add(textCol);
            }

            return dt;
        }
        #endregion
    }
}