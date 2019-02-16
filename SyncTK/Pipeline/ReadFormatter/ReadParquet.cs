using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using SyncTK.Internal;

namespace SyncTK
{
    public class ReadParquet: Component
    {
        public ReadParquet()
        {
        }

        internal override IEnumerable<object> Process(Pipeline pipeline, IEnumerable<object> input)
        {
            foreach (var i in input)
            {
                var stream = (StreamReader)i;
                var reader = new ParquetDataReader(stream);
                
                yield return reader;
            }
        }
    }
}
