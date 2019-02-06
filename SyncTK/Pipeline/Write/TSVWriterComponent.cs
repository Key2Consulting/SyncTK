﻿using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;

namespace SyncTK
{
    public class TSVWriterComponent : WriterComponent
    {
        bool _header = false;

        public TSVWriterComponent(bool header)
        {
            _header = header;
        }

        internal override IEnumerable<object> Process(Sync pipeline, Component upstreamComponent, IEnumerable<object> input)
        {
            foreach (var i in input)
            {
                var reader = (IDataReader)i;
                var writer = new TSVWriter(reader, _header);
                yield return writer;
            }
        }
    }
}