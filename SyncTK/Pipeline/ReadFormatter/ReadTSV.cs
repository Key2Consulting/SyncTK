using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using SyncTK.Internal;

namespace SyncTK
{
    public class ReadTSV : ReadFormatter
    {
        protected const string _delimeter = "\t";
        protected string[] _splitDelimeter;
        protected bool _header = false;
        protected string _firstLine;
        protected string _line;

        public ReadTSV(bool header)
        {
            _header = header;
        }

        protected override void OnBeginReading()
        {
            _splitDelimeter = new string[] { _delimeter };

            // Even if no header is set, we still need to know how many columns there are.
            _firstLine = _reader.ReadLine();
            var columns = _firstLine.Split(_splitDelimeter, StringSplitOptions.None);

            // Foreach of the extract columns from the first row
            for (var i = 0; i < columns.Length; i++)
            {
                // Even though we only support the string data type, we must generate a SchemaTable
                // to adhere to the IDataReader standard (and required by importers).
                var columnSchema = new ColumnSchema()
                {
                    ColumnName = _header ? columns[i] : i.ToString(),       // if no header, use the column ordinal
                    ColumnSize = -1,
                    DataType = typeof(System.String),
                    DataTypeName = "STRING",
                    AllowNull = true
                    // textCol["UdtAssemblyQualifiedName"] = "PowerSync.TextFileDataReader.String";
                };

                _columnSchema.Add(columnSchema);
            }
        }

        protected override void OnBeginFile()
        {
            // If files have headers and we've already read a file, eat the next line so it doesn't end up 
            // in the data stream.
            if (_header && _readCount > 0)
            {
                var line = _reader.ReadLine();
                if (line != _firstLine)
                    throw new Exception("Encountered change in column definitions between multiple text file import.");
            }
        }

        protected override bool OnReadLine()
        {
            // Read the next line from the input stream. If the first line, we've already read it earlier during initialization
            // unless it was the header
            _readCount++;
            string line = "";
            if (_readCount > 1 || _header)
            {
                line = _reader.ReadLine();
            }
            else
            {
                line = _firstLine;
            }

            // If no data was read, we must be at the end of the file.
            if (line == null || line.Trim().Length == 0)
            {
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
    }
}
