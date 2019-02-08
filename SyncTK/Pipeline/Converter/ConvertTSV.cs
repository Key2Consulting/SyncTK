using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;

namespace SyncTK
{
    public class ConvertTSV : Component, IConverter
    {
        bool _header = false;

        public ConvertTSV(bool header)
        {
            _header = header;
        }

        internal override IEnumerable<object> Process(Sync pipeline, Component upstreamComponent, IEnumerable<object> input)
        {
            foreach (var i in input)
            {
                var reader = (TypeConversionReader)i;
                reader.SetTarget(this.GetType());
                var writer = new TSVWriter(reader, _header);
                yield return writer;
            }
        }
    }
}