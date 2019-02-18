using SyncTK;
using System;
using Newtonsoft.Json;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using System.IO;
using System.Reflection;
using Xunit.Abstractions;

namespace SyncTK.Test.UnitTests
{
    public class TestBase
    {
        protected int _sampleFileCount;
        protected int _dataSetSize;
        protected JObject _cfg = null;
        protected readonly ITestOutputHelper _output;
        
        public TestBase(ITestOutputHelper output)
        {
            _output = output;
            _sampleFileCount = int.Parse(GetConfig("SampleFileCount"));
            _dataSetSize = int.Parse(GetConfig("DataSetSize"));
        }

        public string GetConfig(string configKey)
        {
            if (_cfg == null)
            {
                _cfg = JObject.Parse(File.ReadAllText(@"Config.json"));
            }

            return _cfg.GetValue(configKey).ToString();
        }

        public string GetResource(string name)
        {
            var assembly = Assembly.GetExecutingAssembly();
            var param = new object[2];
            param[0] = GetConfig("DataSetSize");
            param[1] = GetConfig("BigStringSize"); 
            var resourceName = "Test.Resources." + name;

            using (Stream stream = assembly.GetManifestResourceStream(resourceName))
            using (StreamReader reader = new StreamReader(stream))
            {
                string resourceData = reader.ReadToEnd();
                return string.Format(resourceData, param);
            }
        }

        protected void WritePipelineOutput(Pipeline pipeline)
        {
            WritePipelineLogEntry(pipeline, "InputFileCount");
            WritePipelineLogEntry(pipeline, "Rows");
            WritePipelineLogEntry(pipeline, "OutputFileCount");
        }

        protected void WritePipelineLogEntry(Pipeline pipeline, string key)
        {
            var find = pipeline.Log.Find(x => x.Key == key);
            if (find.Key != null)
                _output.WriteLine($"...{key} {find.Value}");
        }
    }
}