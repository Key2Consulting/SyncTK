using SyncTK;
using System;
using Newtonsoft.Json;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using System.IO;

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
    }
}
