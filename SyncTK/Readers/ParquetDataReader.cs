using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using Parquet;

namespace SyncTK
{
    internal class ParquetDataReader : IDataReader
    {
        protected System.Collections.ArrayList _columnName;
        protected StreamReader _reader;
        protected int _readCount = 0;
        
        // Parquet.NET specific properties
        protected Parquet.ParquetReader _pqReader;
        protected Parquet.Data.DataField[] _pqDataFields;
        protected Parquet.Data.DataColumn[] _pqDataColumn;
        protected int _rowGroupIndex = 0;
        protected int _rowGroupReadCount = 0;

        public ParquetDataReader(StreamReader reader)
        {
            _reader = reader;

            // Use Parquet.NET to do the heavy reading.
            _pqReader = new Parquet.ParquetReader(reader.BaseStream);
            _pqDataFields = _pqReader.Schema.GetDataFields();

            // Extract the column names from the parquet file in a separate array for fast lookups.
            _columnName = new System.Collections.ArrayList();
            for (int i = 0; i < _pqDataFields.Length; i++)
            {
                _columnName.Add(_pqDataFields[i].Name);
            }
        }

        public object this[int i]
        {
            get
            {
                return _pqDataColumn[i].Data.GetValue(_rowGroupReadCount);
            }
        }

        public object this[string name]
        {
            get
            {
                return _pqDataColumn[_columnName.IndexOf(name)].Data.GetValue(_rowGroupReadCount);
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
            return _pqDataFields[i].DataType.ToString();
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
            return (string)this[i];
        }

        public object GetValue(int i)
        {
            return this[i];
        }

        public int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i)
        {
            return (this[i] == null);
        }

        public bool NextResult()
        {
            throw new NotImplementedException();
        }

        public bool Read()
        {
            // If we still have rows left to read within the current rowgroup.
            if (_pqDataColumn != null && _rowGroupReadCount < _pqDataColumn.Length)
            {
                _readCount++;
                _rowGroupReadCount++;
                return true;
            }
            else if (_rowGroupIndex < _pqReader.RowGroupCount)
            {
                // If we have rowgroups left to read from the overall file.
                _pqDataColumn = _pqReader.ReadEntireRowGroup(_rowGroupIndex++);
                _rowGroupReadCount = 0;
                return true;
            }
            else
            {
                // Otherwise, we're done.
                return false;
            }
        }

        public DataTable GetSchemaTable()
        {
            // TODO

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
                // Default data type properties directly from parquet file.
                Type dataType = null;
                int precision = -1;
                int scale = -1;
                int columnSize = -1;

                // Try to extract additional details about the type.
                switch (_pqDataFields[i].DataType.ToString().ToUpper())
                {
                    case ("DATETIMEOFFSET"):
                        dataType = typeof(DateTimeOffset);
                        columnSize = 10;        // SQL Server uses 10 bytes for DATETIMEOFFSET
                        break;

                    case ("DECIMALDATAFIELD"):
                        var decimalField = (Parquet.Data.DecimalDataField)_pqDataFields[i];
                        dataType = typeof(decimal);
                        columnSize = sizeof(decimal);
                        precision = decimalField.Precision;
                        scale = decimalField.Scale;
                        break;

                    case ("STRING"):
                        dataType = typeof(String);
                        columnSize = -1;        // unlimited
                        break;

                    case ("INT32"):
                        dataType = typeof(Int32);
                        columnSize = sizeof(Int32);
                        break;

                    case ("BOOLEAN"):
                        dataType = typeof(bool);
                        columnSize = sizeof(bool);
                        break;

                    default:
                        throw new Exception($"Unknown Parquet type {_pqDataFields[i].DataType.ToString()}");
                }

                // Add a row describing that column's schema.
                DataRow textCol = dt.NewRow();
                textCol["ColumnName"] = _pqDataFields[i].Name;
                textCol["ColumnOrdinal"] = i;
                textCol["ColumnSize"] = columnSize;
                textCol["DataType"] = dataType;
                textCol["DataTypeName"] = dataType.GetType().Name;
                textCol["AllowDBNull"] = _pqDataFields[i].HasNulls;
                textCol["NumericPrecision"] = precision;
                textCol["NumericScale"] = scale;
                dt.Rows.Add(textCol);
            }

            return dt;
        }
    }
}