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
        protected List<string> _outputFiles = new List<string>();

        public TargetFile(string path)
        {
            _path = path;
        }

        internal override void Validate()
        {
            var upstream = (WriteFormatter)GetUpstreamComponent();
            Assert(upstream._fileRowLimit == 0 || _path.Contains("*"), "Splitting output files requires use of wildcard character * in the path.");
            Assert(GetUpstreamComponent() is WriteFormatter, "Pipeline is missing a WriteFormatter.");
        }

        internal override IEnumerable<object> Process(IEnumerable<object> input)
        {
            // Remaining processing is expected to be handled by converter.
            Assert(input == null, "Unexpected stream sent to TargetFile.");
            return null;
        }

        internal override void End()
        {
            // Dispose of last writer
            if (_streamWriter != null)
            {
                _streamWriter.Dispose();
            }

            // Output log information.
            _pipeline.AddLog("OutputFileCount", _outputFiles.Count);
            foreach (var file in _outputFiles)
                _pipeline.AddLog("OutputFile", file);
        }

        internal virtual StreamWriter GetNextStreamWriter()
        {
            // Dispose of prior writer.
            if (_streamWriter != null)
            {
                _streamWriter.Dispose();
            }

            var nextFilePath = _path.Replace("*", this.GetCurrentTimeStampToken() + "_" + _fileCount.ToString());
            _outputFiles.Add(nextFilePath);
            _streamWriter = new StreamWriter(nextFilePath);
            _fileCount++;

            return _streamWriter;
        }
    }
}