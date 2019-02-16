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
        protected bool _compress;
        protected int _rowGroupRowLimit;
        protected int _rowGroupWriteCount;
        protected List<ArrayList> _buffer = new List<ArrayList>();
        protected List<Type> _dataTypes = new List<Type>();
        protected List<Parquet.Data.DataField> _pqDataFields = new List<Parquet.Data.DataField>();
        protected List<Parquet.Data.DataColumn> _pqDataColumns;
        protected Parquet.Data.Schema _pqSchema;
        protected Parquet.ParquetWriter _pqWriter;

        public WriteParquet() : this(true, 0, 0)
        {
        }

        public WriteParquet(bool compress) : this(compress, 0, 0)
        {
        }

        public WriteParquet(bool compress, int rowGroupRowLimit, int fileRowLimit) : base(fileRowLimit)
        {
            _compress = compress;
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
                _buffer.Add(new ArrayList(_rowGroupRowLimit));
                _dataTypes.Add(map.Target.DataType);
                _pqDataFields.Add(new Parquet.Data.DataField(map.Target.ColumnName, GetNullableType(map.Target.DataType)));
            }

            _pqSchema = new Parquet.Data.Schema(_pqDataFields);
        }

        protected override void OnBeginFile()
        {
            _pqWriter = new Parquet.ParquetWriter(_pqSchema, _writer.BaseStream);

            // Apply compression setting.
            if (_compress)
                _pqWriter.CompressionMethod = CompressionMethod.Gzip;
            else
                _pqWriter.CompressionMethod = CompressionMethod.None;
        }

        protected override void OnEndFile()
        {
            FlushRowGroup();
            _pqWriter.Dispose();
        }

        protected override void OnWriteLine()
        {
            // Write out current record to buffer.
            for (int i = 0; i < _reader.FieldCount; i++)
            {
                var val = _reader.GetValue(i);
                // if (val != null)
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
                var dataType = GetTypeConversionTable()[i].Target.DataType;
                var toType = GetNullableType(dataType);
                var pqCol = new Parquet.Data.DataColumn(_pqDataFields[i], data.ToArray(toType));
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
            _rowGroupWriteCount = 0;
        }

        //// https://stackoverflow.com/questions/108104/how-do-i-convert-a-system-type-to-its-nullable-version
        //Type GetNullableType(Type type)
        //{
        //    // Use Nullable.GetUnderlyingType() to remove the Nullable<T> wrapper if type is already nullable.
        //    var underlyingType = Nullable.GetUnderlyingType(type);
        //    if (underlyingType != null && underlyingType.IsValueType)
        //        return typeof(Nullable<>).MakeGenericType(underlyingType);
        //    else
        //        return type;
        //}
        protected Type GetNullableType(Type TypeToConvert)
        {
            // Abort if no type supplied
            if (TypeToConvert == null)
                return null;

            // If the given type is already nullable, just return it
            if (IsTypeNullable(TypeToConvert))
                return TypeToConvert;

            // If the type is a ValueType and is not System.Void, convert it to a Nullable<Type>
            if (TypeToConvert.IsValueType && TypeToConvert != typeof(void))
                return typeof(Nullable<>).MakeGenericType(TypeToConvert);

            // Done - no conversion
            return null;
        }

        protected bool IsTypeNullable(Type TypeToTest)
        {
            // Abort if no type supplied
            if (TypeToTest == null)
                return false;

            // If this is not a value type, it is a reference type, so it is automatically nullable
            //  (NOTE: All forms of Nullable<T> are value types)
            if (!TypeToTest.IsValueType)
                return true;

            // Report whether TypeToTest is a form of the Nullable<> type
            return TypeToTest.IsGenericType && TypeToTest.GetGenericTypeDefinition() == typeof(Nullable<>);
        }
    }
}