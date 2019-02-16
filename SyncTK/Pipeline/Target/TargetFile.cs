using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using SyncTK.Internal;

namespace SyncTK
{
    public class TargetFile : Target
    {
        protected StreamWriter _streamWriter = null;
        protected string _path = "";
        protected int _fileCount = 0;

        public TargetFile(string path)
        {
            _path = path;
        }

        internal override void Validate(Pipeline pipeline)
        {
            var upstream = (WriteFormatter)GetUpstreamComponent(pipeline);
            Assert(upstream._fileRowLimit == 0 || _path.Contains("*"), "Splitting output files requires use of wildcard character * in the path.");
        }

        internal override IEnumerable<object> Process(Pipeline pipeline, IEnumerable<object> input)
        {
            // Remaining processing is expected to be handled by converter.
            Assert(input == null, "Unexpected stream sent to TargetFile.");
            return null;
        }

        internal override void End(Pipeline pipeline)
        {
            // Dispose of last writer
            if (_streamWriter != null)
            {
                _streamWriter.Dispose();
            }
        }

        internal virtual StreamWriter GetNextStreamWriter()
        {
            // Dispose of prior writer.
            if (_streamWriter != null)
            {
                _streamWriter.Dispose();
            }

            var nextFilePath = _path.Replace("*", this.GetCurrentTimeStampToken() + "_" + _fileCount.ToString());
            _streamWriter = new StreamWriter(nextFilePath);
            _fileCount++;

            return _streamWriter;
        }
    }
}