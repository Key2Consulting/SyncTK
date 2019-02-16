using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using SyncTK.Internal;

namespace SyncTK
{
    public class Pipeline
    {
        #region Pipeline Components
        internal List<Component> _component = new List<Component>();
        #endregion

        #region Default Component Properties
        #endregion

        #region Client Builder Interface
        public Pipeline From(Component connector)
        {
            _component.Add((Component)connector);
            return this;
        }

        public Pipeline ReadFormat(ReadFormatter readFormat)
        {
            _component.Add((Component)readFormat);
            return this;
        }

        public Pipeline WriteFormat(WriteFormatter writeFormat)
        {
            _component.Add((Component)writeFormat);
            return this;
        }

        public Pipeline Into(Component connector)
        {
            _component.Add((Component)connector);
            return this;
        }

        internal T FindComponentType<T>() where T : Component
        {
            for (int i = 0; i < _component.Count; i++)
            {
                if (_component[i] is T || _component[i].GetType().IsSubclassOf(typeof(T)))
                {
                    return (T)_component[i];
                }
            }
            return null;
        }

        public void Exec()
        {
            var source = FindComponentType<Source>();
            var readFormatter = FindComponentType<ReadFormatter>();
            var writeFormatter = FindComponentType<WriteFormatter>();
            var target = FindComponentType<Target>();

            if (source == null)
                throw new Exception("Unable to find source component type.");

            if (target == null)
                throw new Exception("Unable to resolve target component type.");

            // Since moving data involves different types and type systems, always add
            // a TypeConverter to the pipeline which handles this translation. Always 
            // goes after the ReadFormatter (or Source if no ReadFormatter doesn't exist).
            var converter = new TypeConverter(source, readFormatter, writeFormatter, target);
            if (readFormatter != null)
                _component.Insert(_component.IndexOf(readFormatter) + 1, converter);
            else
                _component.Insert(_component.IndexOf(source) + 1, converter);

            // Ensure everyone has a reference back to the pipeline (this).
            foreach (var component in _component)
            {
                component._pipeline = this;
            }

            // Execute the pipeline events in order (Validation, Begin, Process, End).
            // 

            // Pipeline Validation
            foreach (var currentComponent in _component)
            {
                currentComponent.Validate(this);
            }

            // Pipeline Begin
            foreach (var currentComponent in _component)
            {
                currentComponent.Begin(this);
            }

            // Pipeline Process
            IEnumerable<object> previousOutput = null;
            IEnumerable<object> currentOutput = null;
            foreach (var currentComponent in _component)
            {
                currentOutput = currentComponent.Process(this, previousOutput);
                previousOutput = currentOutput;
            }

            // Pipeline End
            foreach (var currentComponent in _component)
            {
                currentComponent.End(this);
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