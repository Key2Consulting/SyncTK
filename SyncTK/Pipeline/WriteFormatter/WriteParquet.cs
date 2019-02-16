using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using Parquet;
using SyncTK.Internal;

namespace SyncTK
{
    /// <summary>
    /// Writes a parquet file.
    /// </summary>
    /// <remarks>
    /// Currently only supports one row group per file, but can produce multiple files.
    /// </remarks>
    public class WriteParquet : WriteFormatter
    {
        protected int _rowGroupRowLimit;
        protected int _rowGroupWriteCount;
        protected List<ArrayList> _buffer = new List<ArrayList>();
        protected List<Type> _dataTypes = new List<Type>();
        protected List<Parquet.Data.DataField> _pqDataFields = new List<Parquet.Data.DataField>();
        protected List<Parquet.Data.DataColumn> _pqDataColumns;
        protected Parquet.Data.Schema _pqSchema;
        protected Parquet.ParquetWriter _pqWriter;

        public WriteParquet() : this(0, 0)
        {
        }
        
        public WriteParquet(int rowGroupRowLimit, int fileRowLimit) : base(fileRowLimit)
        {
            _rowGroupRowLimit = rowGroupRowLimit;
        }

        protected override void OnBeginWriting()
        {
            // Records per Rowgroup unspecified, so auto calculate ideal size which is 1M. However,
            // a large # of columns will consume a lot more memory, so factor that into estimate.
            if (_rowGroupRowLimit == 0)
            {
                if (_pqDataFields.Count < 100)
                {
                    _rowGroupRowLimit = 1000000;
                }
                else if (_pqDataFields.Count < 500)
                {
                    _rowGroupRowLimit = 500000;
                }
                else
                {
                    _rowGroupRowLimit = 250000;
                }
            }

            // Records per file unspecified, so auto calculate targeting 4 row groups per file.
            if (_fileRowLimit == 0)
            {
                _fileRowLimit = _rowGroupRowLimit * 4;
            }

            // Configure column schema information.
            for (int i = 0; i < _reader.FieldCount; i++)
            {
                var map = GetTypeConversionTable()[i];
                _buffer.Add(new ArrayList());
                _dataTypes.Add(map.TargetDataType);
                _pqDataFields.Add(new Parquet.Data.DataField(map.TargetColumnName, map.TargetDataType));
            }

            _pqSchema = new Parquet.Data.Schema(_pqDataFields);
        }

        protected override void OnBeginFile()
        {
            _pqWriter = new Parquet.ParquetWriter(_pqSchema, _writer.BaseStream);
        }

        protected override void OnEndFile()
        {
            FlushRowGroup();
        }

        protected override void OnWriteLine()
        {
            // Write out current record to buffer.
            for (int i = 0; i < _reader.FieldCount; i++)
            {
                var val = _reader.GetValue(i);
                if (val != null)
                    _buffer[i].Add(val);
            }

            // If we've met the row group size requirement, flush it.
            if (++_rowGroupWriteCount >= _rowGroupRowLimit)
                FlushRowGroup();
        }

        protected void FlushRowGroup()
        {
            _pqDataColumns = new List<Parquet.Data.DataColumn>();
            for (int i = 0; i < _reader.FieldCount; i++)
            {
                var data = _buffer[i];
                var pqCol = new Parquet.Data.DataColumn(_pqDataFields[i], data.ToArray(GetTypeConversionTable()[i].TargetDataType));
                _pqDataColumns.Add(pqCol);
            }

            // Create a new row group in the file.
            using (ParquetRowGroupWriter groupWriter = _pqWriter.CreateRowGroup())
            {
                foreach (var pqDataColumn in _pqDataColumns)
                {
                    groupWriter.WriteColumn(pqDataColumn);
                }
            }

            // Cleanup last rowgroup processing.
            _writer.Flush();
            _pqDataColumns = null;
            foreach (var b in _buffer)
            {
                b.Clear();
            }
            GC.Collect();
            _rowGroupWriteCount = 0;
        }
    }
}