using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SyncTK
{
    public class FormatTSV : Component, IFormatter
    {
        bool _header = false;

        public FormatTSV(bool header)
        {
            _header = header;
        }

        internal override IEnumerable<object> Process(Sync pipeline, Component upstreamComponent, IEnumerable<object> input)
        {
            foreach (var i in input)
            {
                var stream = (StreamReader)i;
                var reader = new TypeConversionReader(new TSVReader(stream, _header), this.GetType());
                
                yield return reader;
            }
        }
    }
}
