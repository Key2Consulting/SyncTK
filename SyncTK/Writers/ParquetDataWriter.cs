using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using Parquet;
using System.IO;
using System.Collections;

namespace SyncTK
{
    internal class ParquetDataWriter : IFileWriter
    {
        protected IDataReader _reader;
        protected int _writeCount = 0;
        protected int _previousFileNumber = -1;
        protected List<ArrayList> _buffer = new List<ArrayList>();
        protected List<Type> _dataTypes = new List<Type>();
        protected List<Parquet.Data.DataField> _pqDataFields = new List<Parquet.Data.DataField>();
        protected List<Parquet.Data.DataColumn> _pqDataColumns;
        protected Parquet.Data.Schema _pqSchema;
        protected int _rowGroupWriteCount = 0;
        protected int _rowGroupMaxRecords = 0;
        protected Parquet.ParquetWriter _pqWriter;
        protected TypeConversionTable _typeConversionTable;

        public ParquetDataWriter(IDataReader reader, int rowGroupMaxRecords = -1)
        {
            _reader = reader;

            CreateTypeConversionTable(reader);

            // Configure column schema information.
            var schemaTable = _reader.GetSchemaTable();
            for (int i = 0; i < schemaTable.Rows.Count; i++)
            {
                _buffer.Add(new ArrayList());
                var row = schemaTable.Rows[i];
                var columnName = (string)row["ColumnName"];
                var dataType = (Type)row["DataType"];
                _dataTypes.Add((Type)dataType);

                // Add the Parquet data field.
                if (dataType == typeof(DateTime))
                {
                    _dataTypes[i] = typeof(DateTimeOffset);
                    _pqDataFields.Add(new Parquet.Data.DateTimeDataField(columnName, Parquet.Data.DateTimeFormat.DateAndTime));      // force to use DateTime instead of DateTimeOffset for date types
                }
                else
                    _pqDataFields.Add(new Parquet.Data.DataField(columnName, (Type)dataType));      // let Parquet.NET handle conversion
            }

            _pqSchema = new Parquet.Data.Schema(_pqDataFields);

            // Records per Rowgroup unspecified, so auto calculate ideal size which is 1M. However,
            // a large # of columns will consume a lot more memory, so factor that into estimate.
            if (_pqDataFields.Count < 100)
            {
                _rowGroupMaxRecords = 1000000;
            }
            else if (_pqDataFields.Count < 500)
            {
                _rowGroupMaxRecords = 500000;
            }
            else
            {
                _rowGroupMaxRecords = 250000;
            }
        }

        protected void CreateTypeConversionTable(IDataReader reader)
        {
            _typeConversionTable = new TypeConversionTable(reader.GetSchemaTable());

            foreach (var map in _typeConversionTable)
            {
                var x = map;
                // A variable length type with a size greater than 8K becomes MAX in SQL Server.
                if (map.SourceDataTypeName == "DATETIME")
                {
                    map.TargetDataType = typeof(DateTimeOffset);
                    map.TargetDataTypeName = "DateTimeOffset";
                }

            //    // .NET strings convert to NVARCHAR(MAX)
            //    if (map.SourceDataTypeName == "STRING")
            //    {
            //        map.TargetDataTypeName = "NVARCHAR";
            //        map.TargetColumnSize = -1;
            //    }

            //    // SQL Server special types (Geography, Geometry) require special assembly. However, if these are
            //    // converted to BINARY the assembly isn't required and SQL will convert in target.
            //    switch (map.SourceDataTypeName)
            //    {
            //        case "GEOGRAPHY":
            //            map.TransportAsBinary = true;
            //            break;
            //        case "GEOMETRY":
            //            map.TransportAsBinary = true;
            //            break;
            //        case "HIERARCHYID":
            //            map.TransportAsBinary = true;
            //            break;
            //    }
            }
        }

        public bool Write(StreamWriter writer, int fileReadNumber, int fileWriteNumber)
        {
            // Roll to a new parquet file on FileWriteNumber increment.
            if (_previousFileNumber != fileWriteNumber)
            {
                _previousFileNumber = fileWriteNumber;

                _pqWriter = new Parquet.ParquetWriter(_pqSchema, writer.BaseStream);
                _rowGroupWriteCount = 0;
            }

            // Attempt to read more data from Parquet .NET's reader.
            var moreData = _reader.Read();

            // Close out the row group every X records.
            if (_rowGroupWriteCount++ >= _rowGroupMaxRecords || !moreData)
            {
                _pqDataColumns = new List<Parquet.Data.DataColumn>();
                for (int i = 0; i < _reader.FieldCount; i++)
                {
                    var data = _buffer[i];
                    var pqCol = new Parquet.Data.DataColumn(_pqDataFields[i], data.ToArray(_typeConversionTable[i].TargetDataType));
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
                writer.Flush();
                _pqDataColumns = null;
                foreach (var b in _buffer)
                {
                    b.Clear();
                }
                GC.Collect();
                _rowGroupWriteCount = 0;

                // If that was the last row, exit.
                if (!moreData)
                {
                    return false;
                }
            }

            // Write out current record to buffer.
            for (int i = 0; i < _reader.FieldCount; i++)
            {
                var val = _typeConversionTable.ConvertValue(_reader, i);
                if (val != null)
                    _buffer[i].Add(val);
            }

            return true;
        }
    }
}