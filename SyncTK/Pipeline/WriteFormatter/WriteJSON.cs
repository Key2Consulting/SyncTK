using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Dynamic;
using System.IO;
using System.Text;
using System.Web.Script.Serialization;
using Newtonsoft.Json;
using SyncTK.Internal;

namespace SyncTK
{
    public class WriteJSON : WriteFormatter
    {
        protected const string _newline = "\r\n";

        public WriteJSON() : this(0)
        {
        }

        public WriteJSON(int fileRowLimit) : base(fileRowLimit)
        {
        }

        protected override void OnBeginFile()
        {
            _writer.Write("[");
        }

        protected override void OnEndFile()
        {
            _writer.Write(_newline);
            _writer.Write("]");
        }

        protected override void OnWriteLine()
        {
            if(_fileWriteCount > 0)
            {
                _writer.Write(",");
            }

            _writer.Write(_newline);

            dynamic dataRow = new ExpandoObject();

            // For each data column
            for (int i = 0; i < _reader.FieldCount; i++)
            {
                AddProperty(dataRow, _reader.GetName(i), _reader.GetValue(i));
            }

            var json = JsonConvert.SerializeObject(dataRow);
            _writer.Write(json);
        }

        private void AddProperty(ExpandoObject expando, string name, object value)
        {
            var exDict = expando as IDictionary<string, object>;

            if (exDict.ContainsKey(name))
                exDict[name] = value;
            else
                exDict.Add(name, value);
        }
    }
}