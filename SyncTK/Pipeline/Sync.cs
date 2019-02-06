using System;
using System.Collections.Generic;
using System.Text;

namespace SyncTK
{
    public class Sync
    {
        #region Pipeline Components
        protected List<Component> _component = new List<Component>();
        #endregion

        #region Default Component Properties
        internal int _maxDOP = 3;
        #endregion

        #region Client Builder Interface
        public static Sync Source(SourceComponent connector)
        {
            var sync = new Sync();
            sync._component.Add(connector);
            return sync;
        }

        public Sync Read(ReaderComponent reader)
        {
            _component.Add(reader);
            return this;
        }

        public Sync Write(WriterComponent writer)
        {
            _component.Add(writer);
            return this;
        }

        public Sync Target(TargetComponent connector)
        {
            _component.Add(connector);
            return this;
        }

        public void Exec(int maxDOP = 3)
        {
            _maxDOP = maxDOP;

            // Run component validation
            Component previousComponent = null;
            foreach (var currentComponent in _component)
            {
                currentComponent.Validate(this, previousComponent);
                previousComponent = currentComponent;
            }

            // Component processing
            previousComponent = null;
            IEnumerable<object> previousOutput = null;
            IEnumerable<object> currentOutput = null;
            foreach (var currentComponent in _component)
            {
                currentComponent.Begin(this, previousComponent);
                currentOutput = currentComponent.Process(this, previousComponent, previousOutput);
                currentComponent.End(this, previousComponent);
                previousComponent = currentComponent;
                previousOutput = currentOutput;
            }
        }

        public string SerializeJSON()
        {
            return "";
        }

        public void DeserializeJSON(string JSON)
        {
        }
        #endregion
    }
}