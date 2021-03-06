﻿using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using SyncTK.Internal;

namespace SyncTK
{
    public class WriteTSV : WriteFormatter
    {
        protected bool _header = false;
        protected const string _delimeter = "\t";
        protected const string _newline = "\r\n";

        public WriteTSV() : this(true, 0)
        {
        }

        public WriteTSV(bool header) : this(header, 0)
        {
            _header = header;
        }

        public WriteTSV(bool header, int fileRowLimit) : base(fileRowLimit)
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

                _writer.Write(_reader.GetValue(i));
            }

            // Next line
            _writer.Write(_newline);
        }
    }
}