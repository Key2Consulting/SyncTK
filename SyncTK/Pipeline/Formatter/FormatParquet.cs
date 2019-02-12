using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SyncTK
{
    public class FormatParquet: Component
    {
        public FormatParquet()
        {
        }

        internal override IEnumerable<object> Process(Sync pipeline, Component upstreamComponent, IEnumerable<object> input)
        {
            foreach (var i in input)
            {
                var stream = (StreamReader)i;
                var reader = new TypeConversionReader(new ParquetDataReader(stream), this.GetType());
                
                yield return reader;
            }
        }
    }
}
