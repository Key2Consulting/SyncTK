using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using SyncTK.Internal;

namespace SyncTK
{
    public class ReadCSV : ReadFormatter
    {
        // REGEX from https://stackoverflow.com/questions/18144431/regex-to-split-a-csv
        protected const string _regexParseExpression = @"(?:,""|^"")(""""|[\w\W]*?)(?="",|""$)|(?:,(?!"")|^(?!""))([^,]*?)(?=$|,)|(\r\n|\n)";

        protected bool _header = false;
        protected string _firstLine;
        protected string _line;

        public ReadCSV() : this(true)
        {
        }

        public ReadCSV(bool header)
        {
            _header = header;
        }

        protected override void OnBeginReading()
        {
            // Even if no header is set, we still need to know how many columns there are.
            _firstLine = _reader.ReadLine();

            var matches = Regex.Matches(_firstLine, _regexParseExpression, RegexOptions.IgnoreCase | RegexOptions.Multiline);

            // Foreach of the extract columns from the first row
            for (var i = 0; i < matches.Count; i++)
            {
                // Even though we only support the string data type, we must generate a SchemaTable
                // to adhere to the IDataReader standard (and required by importers).
                var columnSchema = new ColumnSchema()
                {
                    ColumnName = _header ? matches[i].Groups[2].Value : i.ToString(),       // if no header, use the column ordinal
                    ColumnSize = -1,
                    DataType = typeof(System.String),
                    DataTypeName = "STRING",
                    AllowNull = true
                };

                _columnSchema.Add(columnSchema);
            }
        }

        protected override void OnBeginFile()
        {
            // If files have headers and we've already read a file, eat the next line so it doesn't end up 
            // in the data stream.
            if (_header && _totalReadCount > 0)
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
            _totalReadCount++;
            string line = "";
            if (_totalReadCount > 1 || _header)
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

            // CSV must get the values via a regex, because the values themselve may contain commas
            var matches = Regex.Matches(line, _regexParseExpression, RegexOptions.IgnoreCase | RegexOptions.Multiline);

            // Foreach of the extract columns
            for (var i = 0; i < matches.Count; i++)
            {
                if (matches[i].Groups[1].Value.Length > 0)
                {
                    _readBuffer[i] = matches[i].Groups[1].Value;
                }
                else
                {
                    _readBuffer[i] = matches[i].Groups[2].Value;
                }
            }

            return true;
        }
    }
}
