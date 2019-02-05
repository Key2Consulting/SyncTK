using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace SyncTK
{
    public class TSVReader : System.Data.IDataReader
    {
        bool _initialized = false;
        string _delimeter;
        string[] _splitDelimeter;
        bool _header;
        System.Collections.ArrayList _columnName;
        System.Collections.ArrayList _readBuffer;
        IEnumerable<StreamReader> _reader;

        public TSVReader(IEnumerable<StreamReader> reader, bool header)
        {
            this._header = header;
            this._reader = reader;

            // Initialize read operation.
            this.Initialize();
        }

        protected void Initialize()
        {
            this._delimeter = "\t";
            this._splitDelimeter = new string[] { this._delimeter };
        }

        public object this[int i]
        {
            get
            {
                return this._readBuffer[i];
            }
        }

        public object this[string name]
        {
            get
            {
                return this._readBuffer[this._columnName.IndexOf(name)];
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
                return this._reader.BaseStream.Position > 0;
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
                return this._columnName.Count;
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
            return this._columnName[i].ToString();
        }

        public int GetOrdinal(string name)
        {
            return this._columnName.IndexOf(name);
        }

        public string GetString(int i)
        {
            return (string)this._readBuffer[i];
        }

        public object GetValue(int i)
        {
            return this._readBuffer[i];
        }

        public int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i)
        {
            return (this._readBuffer[i] == null);
        }

        public bool NextResult()
        {
            throw new NotImplementedException();
        }

        public bool Read()
        {
            string line = this._reader.ReadLine();        // TODO: how could a row delimeter be applied here?
            try
            {
                if (!this._initialized) {
                    this._initialized = true;

                    // Even if no header is set, we still need to know how many columns there are.
                    var columns = line.Split(this._splitDelimeter, StringSplitOptions.None);
                    this._columnName = new System.Collections.ArrayList(columns.Length);
                    this._readBuffer = new System.Collections.ArrayList(columns.Length);           // preallocate once and only once for performance

                    // Foreach of the extract columns from the first row
                    for (var i = 0; i < columns.Length; i++)
                    {
                        this._columnName.Add(columns[i]);
                        this._readBuffer.Add(null);

                        // If we don't have a header, use the column number as the name
                        if (!this._header)
                        {
                            this._columnName[i] = i.ToString();
                        }
                    }

                    // If header isn't first line, must reset read back to beginning.
                    if (!this._header)
                    {
                        this._reader.BaseStream.Position = 0;
                        this._reader.DiscardBufferedData();
                    }
                }
                
                if (line == null || line.Trim().Length == 0)
                {
                    this._reader.Close();
                    return false;
                }

                // Otherwise, use simple delimeter parsing (i.e. Split)
                var columns = line.Split(this._splitDelimeter, StringSplitOptions.None);

                // Foreach of the extract columns
                for (var i = 0; i < columns.Length; i++)
                {
                    this._readBuffer[i] = columns[i];
                }

                return true;
            }
            catch (Exception ex)
            {
                line = this._reader.ReadLine();
                throw new Exception(line, ex);
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
            for (int i = 0; i < this._columnName.Count; i++)
            {
                // Add a row describing that column's schema.
                DataRow textCol = dt.NewRow();
                textCol["ColumnName"] = this._columnName[i];
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