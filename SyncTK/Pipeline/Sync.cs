using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace SyncTK
{
    public class Sync
    {
        #region Pipeline Components
        internal List<Component> _component = new List<Component>();
        #endregion

        #region Default Component Properties
        #endregion

        #region Client Builder Interface
        public Sync From(Component connector)
        {
            _component.Add((Component)connector);
            return this;
        }

        public Sync WithFormat(Component reader)
        {
            _component.Add((Component)reader);
            return this;
        }

        public Sync ConvertTo(Component writer)
        {
            _component.Add((Component)writer);
            return this;
        }

        public Sync Into(Component connector)
        {
            _component.Add((Component)connector);
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

            // Pipeline Begin
            previousComponent = null;
            foreach (var currentComponent in _component)
            {
                currentComponent.Begin(this, previousComponent);
                previousComponent = currentComponent;
            }

            // Pipeline Process
            IEnumerable<object> previousOutput = null;
            IEnumerable<object> currentOutput = null;
            previousComponent = null;
            foreach (var currentComponent in _component)
            {
                currentOutput = currentComponent.Process(this, previousComponent, previousOutput);
                previousComponent = currentComponent;
                previousOutput = currentOutput;
            }

            // Pipeline End
            previousComponent = null;
            foreach (var currentComponent in _component)
            {
                currentComponent.End(this, previousComponent);
                previousComponent = currentComponent;
            }
        }

        public Task ExecAsync()
        {
            return new Task(() =>
            {
                this.Exec();
            });
        }
       #endregion
    }
}