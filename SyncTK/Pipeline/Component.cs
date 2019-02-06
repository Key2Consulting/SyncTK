using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace SyncTK
{
    public abstract class Component
    {
        protected string _name = null;

        public Component()
        {
        }

        public Component(string name)
        {
            _name = name;
        }

        internal virtual void Validate(Sync pipeline, Component upstreamComponent)
        {
        }

        internal virtual void Begin(Sync pipeline, Component upstreamComponent)
        {
        }

        internal virtual IEnumerable<object> Process(Sync pipeline, Component upstreamComponent, IEnumerable<object> input)
        {
            throw new NotImplementedException();
        }

        internal virtual void End(Sync pipeline, Component upstreamComponent)
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