using System;
using System.Collections.Generic;
using System.Text;

namespace SyncTK
{
    public class Sync
    {
        #region Pipeline Components
        protected List<Component> _source = new List<Component>();
        protected List<Component> _target = new List<Component>();
        protected List<Component> _reader = new List<Component>();
        protected List<Component> _writer = new List<Component>();
        protected List<Component> _extractQuery = new List<Component>();
        #endregion

        #region Default Component Properties
        #endregion

        #region Client Builder Interface
        public Sync(Connector connector)
        {
            _source.Add(connector);
        }

        public static Sync Source(Connector connector)
        {
            return new Sync(connector);
        }

        public Sync Target(Connector connector)
        {
            _target.Add(connector);
            return this;
        }

        public Sync Read(Reader reader)
        {
            _reader.Add(reader);
            return this;
        }

        public Sync Write(Writer writer)
        {
            _writer.Add(writer);
            return this;
        }
        #endregion
    }
}
