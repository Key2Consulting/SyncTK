using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SyncTK
{
    public class FileTargetComponent : TargetComponent
    {
        protected string _path = "";
        protected int _fileRowCountLimit = 0;
        protected int _fileNumber = 0;
        protected int _rowCounter = 0;
        protected StreamWriter _streamWriter = null;

        public FileTargetComponent(string path)
        {
            _path = path;
        }

        public FileTargetComponent(string path, int fileRowCountLimit)
        {
            _path = path;
            _fileRowCountLimit = fileRowCountLimit;
        }

        internal override void Validate(Sync pipeline, Component upstreamComponent)
        {
            this.Assert(_fileRowCountLimit == 0 || _path.Contains("*"), "Splitting output files requires use of wildcard character * in the path.");
        }

        internal override IEnumerable<object> Process(Sync pipeline, Component upstreamComponent, IEnumerable<object> input)
        {
            foreach (var i in input)
            {
                var writer = (IDataWriter)i;

                while (writer.Write(GetNextStreamWriter())) { }
            }

            // Targets don't produce output during processing.
            return null;
        }

        internal override void End(Sync pipeline, Component upstreamComponent)
        {
            // Dispose of last writer
            if (_streamWriter != null)
            {
                _streamWriter.Flush();
                _streamWriter.Close();
                _streamWriter.Dispose();
            }
        }

        protected StreamWriter GetNextStreamWriter()
        {
            _rowCounter++;

            // If we've met our split limit
            if (_rowCounter >= _fileRowCountLimit && _fileRowCountLimit > 0 || _streamWriter == null)
            {
                if (_streamWriter != null)
                {
                    _streamWriter.Flush();
                    _streamWriter.Close();
                    _streamWriter.Dispose();
                }
                _fileNumber++;
                _streamWriter = new StreamWriter(_path.Replace("*", _fileNumber.ToString()));                
            }

            return _streamWriter;
        }
    }
}
