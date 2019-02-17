using SyncTK;
using System;
using Newtonsoft.Json;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using System.IO;
using System.Reflection;

namespace SyncTK.Test.UnitTests
{
    public class TestBase
    {
        protected JObject _cfg = null;

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
    }
}