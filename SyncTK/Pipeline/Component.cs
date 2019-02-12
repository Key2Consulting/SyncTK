using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace SyncTK
{
    public abstract class Component
    {
        public Component()
        {
        }

        internal virtual void Begin(Sync pipeline, Component upstreamComponent)
        {
        }

        internal virtual void End(Sync pipeline, Component upstreamComponent)
        {
        }

        internal virtual IEnumerable<object> Process(Sync pipeline, Component upstreamComponent, IEnumerable<object> input)
        {
            throw new NotImplementedException();
        }

        internal virtual void Validate(Sync pipeline, Component upstreamComponent)
        {
        }

        protected void Assert(bool condition, string errorMessage)
        {
            if (!condition)
            {
                throw new Exception(errorMessage);
            }
        }

        protected string GetCurrentTimeStampToken()
        {
            return System.DateTime.Now.ToString("yyyyMMdd_mmssfff");
        }


    }
}