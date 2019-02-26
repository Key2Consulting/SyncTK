using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Dynamic;
using System.IO;
using System.Text;
using System.Web.Script.Serialization;
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

            var serializer = new JavaScriptSerializer();
            serializer.RegisterConverters(new JavaScriptConverter[] { new ExpandoJSONConverter() });
            var json = serializer.Serialize(dataRow);

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

    //special serializer that will flatten out our expando object in the manner we want
    public class ExpandoJSONConverter : JavaScriptConverter
    {
        public override IEnumerable<Type> SupportedTypes
        {
            get
            {
                return new ReadOnlyCollection<Type>(new Type[] { typeof(System.Dynamic.ExpandoObject) });
            }
        }

        public override IDictionary<string, object> Serialize(object obj, JavaScriptSerializer serializer)
        {
            var result = new Dictionary<string, object>();
            var dictionary = obj as IDictionary<string, object>;
            foreach (var item in dictionary)
            {
                //format DateTime into the "preferred" format for JSON (ISO 8601)
                if (item.Value is DateTimeOffset)
                {
                    var formattedDateString = ((DateTimeOffset)item.Value).ToString("yyyy-MM-ddTHH\\:mm\\:ss.fffffffzzz"); ;
                    result.Add(item.Key, formattedDateString);
                }
                else
                {
                    result.Add(item.Key, item.Value);
                }
            }
            return result;
        }

        public override object Deserialize(IDictionary<string, object> dictionary, Type type, JavaScriptSerializer serializer)
        {
            throw new NotImplementedException();
        }
    }
}