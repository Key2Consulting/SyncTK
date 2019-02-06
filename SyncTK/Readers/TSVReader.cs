using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace SyncTK
{
    internal class TSVReader : System.Data.IDataReader
    {
        protected const string _delimeter = "\t";
        protected string[] _splitDelimeter;
        protected bool _header;
        protected System.Collections.ArrayList _columnName;
        protected System.Collections.ArrayList _readBuffer;
        protected StreamReader _reader;
        protected int _readCount = 0;
        protected string _firstLine = "";

        public TSVReader(System.IO.StreamReader reader, bool header)
        {
            _reader = reader;
            _header = header;
            _splitDelimeter = new string[] { _delimeter };

            // Even if no header is set, we still need to know how many columns there are.
            _firstLine = _reader.ReadLine();
            var columns = _firstLine.Split(_splitDelimeter, StringSplitOptions.None);
            _columnName = new System.Collections.ArrayList(columns.Length);
            _readBuffer = new System.Collections.ArrayList(columns.Length);           // preallocate once and only once for performance

            // Foreach of the extract columns from the first row
            for (var i = 0; i < columns.Length; i++)
            {
                _columnName.Add(columns[i]);
                _readBuffer.Add(null);

                // If we don't have a header, use the column number as the name
                if (!_header)
                {
                    _columnName[i] = i.ToString();
                }
            }
        }

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
                return _readBuffer[_columnName.IndexOf(name)];
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
                return _reader.BaseStream.Position > 0;
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
                return _columnName.Count;
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
            return _columnName.IndexOf(name);
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
            // Read the next line from the input stream. If the first line, we've already read it earlier during initialization.
            _readCount++;
            string line = "";
            if (_readCount > 0)
            {
                line = _reader.ReadLine();
            }
            else
            {
                line = _firstLine;
            }

            try
            {
                // If no data was read, we must be at the end of the file.
                if (line == null || line.Trim().Length == 0)
                {
                    _reader.Close();
                    return false;
                }

                // Use simple delimeter parsing (i.e. Split) with TSV.
                var columns = line.Split(_splitDelimeter, StringSplitOptions.None);

                // If first record, initialize. Must initialize within our read logic since we're streaming and
                // we need to gather some basic information about the data such as column count.

                // Foreach of the extract columns
                for (var i = 0; i < columns.Length; i++)
                {
                    _readBuffer[i] = columns[i];
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
            for (int i = 0; i < _columnName.Count; i++)
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
    }
}