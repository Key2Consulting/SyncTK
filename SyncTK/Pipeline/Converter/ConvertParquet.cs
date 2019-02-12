using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;

namespace SyncTK
{
    public class ConvertParquet : Component
    {
        protected int _rowGroupMaxRecords = 0;

        public ConvertParquet()
        {
            _rowGroupMaxRecords = -1;
        }

        public ConvertParquet(int rowGroupMaxRecords = 1000000)
        {
            _rowGroupMaxRecords = rowGroupMaxRecords;
        }

        internal override IEnumerable<object> Process(Sync pipeline, Component upstreamComponent, IEnumerable<object> input)
        {
            foreach (var i in input)
            {
                var reader = (TypeConversionReader)i;
                reader.SetTarget(this.GetType());
                var writer = new ParquetDataWriter(reader, _rowGroupMaxRecords);
                yield return writer;
            }
        }
    }
}