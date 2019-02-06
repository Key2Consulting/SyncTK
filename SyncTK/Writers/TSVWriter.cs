using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;

namespace SyncTK
{
    internal class TSVWriter : IDataWriter
    {
        protected const string _delimeter = "\t";
        protected bool _header;
        protected IDataReader _reader;
        protected int _writeCount = 0;
        protected const string _newline = "\r\n";

        public TSVWriter(IDataReader reader, bool header)
        {
            _reader = reader;
            _header = header;
        }

        public bool Write(StreamWriter writer)
        {
            try
            {
                // If first row
                if (_writeCount == 0)
                {
                    // If writing the header
                    if (_header)
                    {
                        // For each column
                        for (int i = 0; i < _reader.FieldCount; i++)
                        {
                            // If not the first column, write the delimeter
                            if (i > 0)
                            {
                                writer.Write(_delimeter);
                            }
                            writer.Write(_reader.GetName(i));
                        }

                        // Next line
                        writer.Write(_newline);
                    }
                }

                // For each data row
                if (_reader.Read())
                {
                    _writeCount++;

                    // For each data column
                    for (int i = 0; i < _reader.FieldCount; i++)
                    {
                        // If not the first column, write the delimeter
                        if (i > 0)
                        {
                            writer.Write(_delimeter);
                        }

                        writer.Write(_reader.GetValue(i));
                    }

                    // Next line
                    writer.Write(_newline);

                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Error in TSVWriter writing line {_writeCount}.", ex);
            }
        }
    }
}