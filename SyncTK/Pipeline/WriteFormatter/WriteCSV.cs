using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using SyncTK.Internal;

namespace SyncTK
{
    public class WriteCSV : WriteFormatter
    {
        protected bool _header = false;
        protected const string _delimeter = ",";
        protected const string _quote = "\"";
        protected const string _quoteEscape = "\"\"";
        protected const string _newline = "\r\n";

        public WriteCSV() : this(true, 0)
        {
        }

        public WriteCSV(bool header) : this(header, 0)
        {
            _header = header;
        }

        public WriteCSV(bool header, int fileRowLimit) : base(fileRowLimit)
        {
            _header = header;
        }

        protected override void OnBeginFile()
        {
            // If header is specified, write the header to initialize new file.
            if (_header)
            {
                // For each column.
                for (int i = 0; i < _reader.FieldCount; i++)
                {
                    // If not the first column, write the delimeter
                    if (i > 0)
                    {
                        _writer.Write(_delimeter);
                    }

                    _writer.Write(_reader.GetName(i));
                }

                // Next line
                _writer.Write(_newline);
            }
        }

        protected override void OnWriteLine()
        {
            // For each data column
            for (int i = 0; i < _reader.FieldCount; i++)
            {
                // If not the first column, write the delimeter
                if (i > 0)
                {
                    _writer.Write(_delimeter);
                }

                var valString = _reader.GetValue(i).ToString();

                //we must wrap certain values in quotes, in the event the value itself contain commas
                //that means we must also escape quotes within those value strings
                if (valString.Contains(","))
                {
                    var val = _quote + valString.Replace(_quote, _quoteEscape) + _quote;
                    _writer.Write(val);
                }
                else
                {
                    _writer.Write(valString);
                }
            }

            // Next line
            _writer.Write(_newline);
        }
    }
}