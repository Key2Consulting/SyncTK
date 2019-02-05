using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using SyncTK;

namespace SyncTK
{
    public abstract class Connector
    {
        public Connector()
        {
        }

        public Connector(string name)
        {
            // TODO: Load from JSON
        }
    }
}