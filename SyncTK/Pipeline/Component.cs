using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace SyncTK
{
    public abstract class Component
    {
        protected string _name = null;

        protected ParallelOptions ParallelOptions
        {
            get
            {
                var options = new ParallelOptions();
                options.MaxDegreeOfParallelism = 3;
                return options;
            }
        }

        public Component()
        {
        }

        public Component(string name)
        {
            _name = name;
        }

        protected virtual void Validate(Sync pipeline, Component upstreamComponent)
        {
        }

        protected virtual void Begin(Sync pipeline, Component upstreamComponent)
        {
        }

        protected virtual IEnumerable<object> Process(Sync pipeline, Component upstreamComponent, IEnumerable<object> input)
        {
            throw new NotImplementedException();
        }

        protected virtual void End(Sync pipeline, Component upstreamComponent)
        {
        }

        protected void Assert(bool condition, string errorMessage)
        {
            if (!condition)
            {
                throw new Exception(errorMessage);
            }
        }
    }
}