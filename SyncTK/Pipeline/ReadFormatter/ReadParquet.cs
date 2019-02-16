using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using SyncTK.Internal;

namespace SyncTK
{
    public class ReadParquet: ReadFormatter
    {
        protected Parquet.ParquetReader _pqReader;
        protected Parquet.Data.DataField[] _pqDataFields;
        protected Parquet.Data.DataColumn[] _pqDataColumn;
        protected int _rowGroupIndex = 0;
        protected int _rowGroupReadCount = 0;
        protected int _rowGroupRowCount = 0;

        public ReadParquet()
        {
        }

        protected override void OnBeginReading()
        {
            // Use Parquet.NET to do the heavy reading.
            var options = new Parquet.ParquetOptions();
            _pqReader = new Parquet.ParquetReader(_reader.BaseStream);
            _pqDataFields = _pqReader.Schema.GetDataFields();

            // Extract the column names from the parquet file in a separate array for fast lookups.
            for (int i = 0; i < _pqDataFields.Length; i++)
            {
                var columnSchema = new ColumnSchema()
                {
                    ColumnName = _pqDataFields[i].Name,
                    ColumnSize = -1,
                    NumericPrecision = -1,
                    NumericScale = -1,
                    AllowNull = _pqDataFields[i].HasNulls
                };

                // Try to extract additional details about the type.
                switch (_pqDataFields[i].DataType.ToString().ToUpper())
                {
                    case ("DATETIMEOFFSET"):
                        columnSchema.DataType = typeof(DateTimeOffset);
                        break;

                    case ("DECIMALDATAFIELD"):
                        columnSchema.DataType = typeof(decimal);
                        columnSchema.ColumnSize = sizeof(decimal);
                        columnSchema.NumericPrecision = (short)((Parquet.Data.DecimalDataField)_pqDataFields[i]).Precision;
                        columnSchema.NumericScale = (short)((Parquet.Data.DecimalDataField)_pqDataFields[i]).Scale;
                        break;

                    case ("DECIMAL"):
                        columnSchema.DataType = typeof(decimal);
                        columnSchema.ColumnSize = sizeof(decimal);
                        columnSchema.NumericPrecision = 38;
                        columnSchema.NumericScale = 18;
                        break;

                    case ("FLOAT"):
                        columnSchema.DataType = typeof(float);
                        columnSchema.ColumnSize = sizeof(float);
                        break;

                    case ("STRING"):
                        columnSchema.DataType = typeof(string);
                        break;

                    case ("BYTE"):
                        columnSchema.DataType = typeof(byte);
                        columnSchema.ColumnSize = sizeof(byte);
                        break;

                    case ("SHORT"):
                        columnSchema.DataType = typeof(short);
                        columnSchema.ColumnSize = sizeof(short);
                        break;

                    case ("INT32"):
                        columnSchema.DataType = typeof(Int32);
                        columnSchema.ColumnSize = sizeof(Int32);
                        break;

                    case ("INT64"):
                        columnSchema.DataType = typeof(Int64);
                        columnSchema.ColumnSize = sizeof(Int64);
                        break;

                    case ("DOUBLE"):
                        columnSchema.DataType = typeof(double);
                        columnSchema.ColumnSize = sizeof(double);
                        break;

                    case ("BOOLEAN"):
                        columnSchema.DataType = typeof(bool);
                        columnSchema.ColumnSize = sizeof(bool);
                        break;

                    case ("BYTEARRAY"):
                        columnSchema.DataType = typeof(byte[]);
                        break;

                    default:
                        throw new Exception($"Unknown Parquet type {_pqDataFields[i].DataType.ToString()}");
                }
                
                columnSchema.DataTypeName = columnSchema.DataType.Name;

                _columnSchema.Add(columnSchema);
            }
        }

        protected override bool OnReadLine()
        {
            bool hasData = true;

            // If we still have rows left to read within the current rowgroup.
            if (_pqDataColumn != null && _rowGroupReadCount < _rowGroupRowCount)
            {
                _readCount++;
            }
            else if (_rowGroupIndex < _pqReader.RowGroupCount)
            {
                // If we have rowgroups left to read from the overall file.
                _pqDataColumn = _pqReader.ReadEntireRowGroup(_rowGroupIndex++);
                _rowGroupRowCount = _pqDataColumn[0].Data.Length;
                _rowGroupReadCount = 0;
            }
            else
            {
                // Otherwise, we're done.
                hasData = false;
            }

            // If we have data, load into the read buffer.
            if (hasData)
            {
                for (int i = 0; i < _pqDataColumn.Length; i++)
                {
                    Array data = _pqDataColumn[i].Data;
                    _readBuffer[i] = data.GetValue(_rowGroupReadCount);
                }

                _rowGroupReadCount++;
            }

            return hasData;
        }
    }
}
