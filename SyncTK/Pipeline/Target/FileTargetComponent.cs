using System;
using System.Collections.Generic;
using System.Text;

namespace SyncTK
{
    internal class FileTargetComponent : TargetComponent
    {
        protected string _path = "";
        protected int _splitSize = 0;

        public FileTargetComponent(string path)
        {
            _path = path;
        }

        public FileTargetComponent(string path, int splitSize)
        {
            _path = path;
            _splitSize = splitSize;
        }

        internal override IEnumerable<object> Process(Sync pipeline, Component upstreamComponent, IEnumerable<object> input)
        {
            foreach (var i in input)
            {
                var writer = (IDataWriter)i;
                int rowCounter = 0;
                var streamWriter = new File
                while (writer.Write(null))
                {
                    rowCounter++;
                    if (rowCounter >= _splitSize && _splitSize > 0)
                    {

                    }
                }
            }

            // Targets don't produce output during processing.
            return null;
        }

        protected string GetNextPath()
        {
        }
    }
}
