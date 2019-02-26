using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text;
using System.Web.Script.Serialization;
using SyncTK.Internal;

namespace SyncTK
{
    public class ReadJSON : ReadFormatter
    {
        protected string _json;
        protected dynamic _jsonObj;
        protected bool _firstFile = true;
        protected int _fileReadCount = 0;

        public ReadJSON()
        {
        }

        protected override void OnBeginReading()
        {
            _json = _reader.ReadToEnd();

            var serializer = new JavaScriptSerializer();
            serializer.RegisterConverters(new[] { new DynamicJsonConverter() });
            _jsonObj = serializer.Deserialize(_json, typeof(object));

            //build the column info from the first record
            if(_jsonObj.Length > 0)
            {
                DynamicJsonObject obj = _jsonObj[0];

                foreach (var key in obj.Dictionary.Keys)
                {
                    var columnSchema = new ColumnSchema()
                    {
                        ColumnName = key,
                        ColumnSize = -1,
                        DataType = obj.Dictionary[key].GetType(),
                        DataTypeName = obj.Dictionary[key].GetType().ToString(),
                        AllowNull = true
                    };

                    _columnSchema.Add(columnSchema);
                }
            }
        }

        protected override void OnBeginFile()
        {
            _fileReadCount = 0;

            //we don't need to build the data object for the first file here, because it was done when we
            //extracted the column info in OnBeginReading
            if (_firstFile)
            {
                _firstFile = false;
            }
            else
            {
                _json = _reader.ReadToEnd();

                var serializer = new JavaScriptSerializer();
                serializer.RegisterConverters(new[] { new DynamicJsonConverter() });
                _jsonObj = serializer.Deserialize(_json, typeof(object));
            }
        }

        //we don't process JSON files line by line, the entire file has already been read in all at once
        //but we will build up the read buffer record by record
        protected override bool OnReadLine()
        {
            // If no data is left, we must be at the end of the file.
            if (_fileReadCount >= _jsonObj.Length)
            {
                return false;
            }

            DynamicJsonObject obj = _jsonObj[_fileReadCount];

            var i = 0;
            foreach (var key in obj.Dictionary.Keys)
            {
                _readBuffer[i] = obj.Dictionary[key];
                i++;
            }

            _fileReadCount++;
            _totalReadCount++;
            return true;
        }
    }

    public class DynamicJsonObject : DynamicObject
    {
        public IDictionary<string, object> Dictionary { get; set; }

        public DynamicJsonObject(IDictionary<string, object> dictionary)
        {
            this.Dictionary = dictionary;
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = this.Dictionary[binder.Name];

            if (result is IDictionary<string, object>)
            {
                result = new DynamicJsonObject(result as IDictionary<string, object>);
            }
            else if (result is ArrayList && (result as ArrayList) is IDictionary<string, object>)
            {
                result = new List<DynamicJsonObject>((result as ArrayList).ToArray().Select(x => new DynamicJsonObject(x as IDictionary<string, object>)));
            }
            else if (result is ArrayList)
            {
                result = new List<object>((result as ArrayList).ToArray());
            }

            return this.Dictionary.ContainsKey(binder.Name);
        }
    }

    public class DynamicJsonConverter : JavaScriptConverter
    {
        public override IEnumerable<Type> SupportedTypes
        {
            get { return new ReadOnlyCollection<Type>(new List<Type>(new Type[] { typeof(object) })); }
        }

        public override IDictionary<string, object> Serialize(object obj, JavaScriptSerializer serializer)
        {
            throw new NotImplementedException();
        }

        public override object Deserialize(IDictionary<string, object> dictionary, Type type, JavaScriptSerializer serializer)
        {
            if (dictionary == null)
                throw new ArgumentNullException("dictionary");

            if (type == typeof(object))
            {
                return new DynamicJsonObject(dictionary);
            }

            return null;
        }
    }
}
