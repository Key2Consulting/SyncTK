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
                    DataTypeName = _pqDataFields[i].DataType.ToString(),
                    ColumnSize = -1,
                    NumericPrecision = -1,
                    NumericScale = -1,
                    AllowNull = _pqDataFields[i].HasNulls
                };

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
