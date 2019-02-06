using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace SyncTK
{
    public class Sync
    {
        #region Pipeline Components
        protected List<Component> _component = new List<Component>();
        #endregion

        #region Default Component Properties
        #endregion

        #region Client Builder Interface
        public static Sync From(SourceComponent connector)
        {
            var sync = new Sync();
            sync._component.Add(connector);
            return sync;
        }

        public Sync WithFormat(FormatComponent reader)
        {
            _component.Add(reader);
            return this;
        }

        public Sync ConvertTo(ConvertComponent writer)
        {
            _component.Add(writer);
            return this;
        }

        public Sync Into(TargetComponent connector)
        {
            _component.Add(connector);
            return this;
        }

        public void Exec()
        {
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

        public Task ExecAsync()
        {
            return new Task(() =>
            {
                this.Exec();
            });
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