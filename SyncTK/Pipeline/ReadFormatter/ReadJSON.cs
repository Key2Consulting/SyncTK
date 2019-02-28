using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text;
using System.Web.Script.Serialization;
using Newtonsoft.Json.Linq;
using SyncTK.Internal;

namespace SyncTK
{
    public class ReadJSON : ReadFormatter
    {
        protected string _json;
        protected dynamic _jsonObjArray;
        protected bool _firstFile = true;
        protected int _fileReadCount = 0;

        public ReadJSON()
        {
        }

        protected override void OnBeginReading()
        {
            _json = _reader.ReadToEnd();
            _jsonObjArray = JArray.Parse(_json);

            //build the column info from the first record
            if(_jsonObjArray.Count > 0)
            {
                JObject jobj = _jsonObjArray[0];

                foreach (var item in jobj)
                {
                    string name = item.Key;
                    JValue jvalue = (JValue)item.Value;

                    var columnSchema = new ColumnSchema()
                    {
                        ColumnName = item.Key,
                        ColumnSize = -1,
                        DataType = jvalue.Value.GetType(),
                        DataTypeName = jvalue.Value.GetType().ToString(),
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
                _jsonObjArray = JArray.Parse(_json);
            }
        }

        //we don't process JSON files line by line, the entire file has already been read in all at once
        //but we will build up the read buffer record by record
        protected override bool OnReadLine()
        {
            // If no data is left, we must be at the end of the file.
            if (_fileReadCount >= _jsonObjArray.Count)
            {
                return false;
            }

            JObject jobj = _jsonObjArray[_fileReadCount];

            var i = 0;
            foreach (var item in jobj)
            {
                JValue jvalue = (JValue)item.Value;
                _readBuffer[i] = jvalue.Value;
                i++;
            }

            _fileReadCount++;
            _totalReadCount++;
            return true;
        }
    }
}
