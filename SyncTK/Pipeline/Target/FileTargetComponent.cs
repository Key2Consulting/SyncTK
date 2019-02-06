using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SyncTK
{
    public class FileTargetComponent : TargetComponent
    {
        protected string _path = "";
        protected int _splitCount = 0;
        protected int _fileNumber = 0;

        public FileTargetComponent(string path)
        {
            _path = path;
        }

        public FileTargetComponent(string path, int splitSize)
        {
            _path = path;
            _splitCount = splitSize;
        }

        internal override void Validate(Sync pipeline, Component upstreamComponent)
        {
            this.Assert(_splitCount == 0 || _path.Contains("*"), "Splitting output files requires use of wildcard character * in the path.");
        }
        internal override IEnumerable<object> Process(Sync pipeline, Component upstreamComponent, IEnumerable<object> input)
        {
            foreach (var i in input)
            {
                var writer = (IDataWriter)i;
                int rowCounter = 0;
                var streamWriter = new StreamWriter(GetNextPath());
                while (writer.Write(streamWriter))
                {
                    rowCounter++;
                    
                    // If we've met our split limit
                    if (rowCounter >= _splitCount && _splitCount > 0)
                    {
                        streamWriter.Flush();
                        streamWriter.Close();
                        streamWriter.Dispose();
                        streamWriter = new StreamWriter(GetNextPath());
                    }
                }

                // Dispose of last writer
                streamWriter.Flush();
                streamWriter.Close();
                streamWriter.Dispose();
            }

            // Targets don't produce output during processing.
            return null;
        }

        protected string GetNextPath()
        {
            _fileNumber++;
            return _path.Replace("*", _fileNumber.ToString());
        }
    }
}
