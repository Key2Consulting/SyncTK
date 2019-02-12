using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace SyncTK
{
    public class TargetFile : ConnectorFile
    {
        protected string _path = "";
        protected int _fileRowLimit = 0;
        protected int _fileReadNumber = 0;
        protected int _fileWriteNumber = 0;
        protected int _rowCounter = 0;
        protected StreamWriter _streamWriter = null;

        public TargetFile(string path)
        {
            _path = path;
        }

        public TargetFile(string path, int fileRowLimit)
        {
            _path = path;
            _fileRowLimit = fileRowLimit;
        }

        internal override void Validate(Sync pipeline, Component upstreamComponent)
        {
            this.Assert(_fileRowLimit == 0 || _path.Contains("*"), "Splitting output files requires use of wildcard character * in the path.");
        }

        internal override IEnumerable<object> Process(Sync pipeline, Component upstreamComponent, IEnumerable<object> input)
        {
            foreach (var i in input)
            {
                var writer = (IDataWriter)i;
                while (writer.Write(GetNextStreamWriter(), _fileReadNumber, _fileWriteNumber)) { }
                _fileReadNumber++;
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
            if (_rowCounter >= _fileRowLimit && _fileRowLimit > 0 || _streamWriter == null)
            {
                // Dispose of prior writer.
                if (_streamWriter != null)
                {
                    _streamWriter.Flush();
                    _streamWriter.Close();
                    _streamWriter.Dispose();
                }
                _fileWriteNumber++;
                _streamWriter = new StreamWriter(_path.Replace("*", this.GetCurrentTimeStampToken() + _fileWriteNumber.ToString()));
            }

            return _streamWriter;
        }
    }
}