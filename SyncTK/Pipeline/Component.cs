using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace SyncTK.Internal
{
    public abstract class Component
    {
        internal Pipeline _pipeline;
        private TypeConversionTable _typeConversionTable;

        internal Component()
        {
        }

        internal virtual void Begin(Pipeline pipeline)
        {
        }

        internal virtual void End(Pipeline pipeline)
        {
        }

        internal virtual IEnumerable<object> Process(Pipeline pipeline, IEnumerable<object> input)
        {
            throw new NotImplementedException();
        }

        internal virtual void Validate(Pipeline pipeline)
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

        protected Component GetUpstreamComponent(Pipeline pipeline)
        {
            var currentIndex = pipeline._component.IndexOf(this);
            if (currentIndex > 0)
                return pipeline._component[currentIndex - 1];
            return null;
        }

        protected Component GetDownstreamComponent(Pipeline pipeline)
        {
            var currentIndex = pipeline._component.IndexOf(this);
            if (currentIndex < pipeline._component.Count - 1)
                return pipeline._component[currentIndex + 1];
            return null;
        }

        internal TypeConversionTable GetTypeConversionTable()
        {
            // Extract the conversion table. Must do this within the iteration since upstream components may use yield.
            if (_typeConversionTable == null)
            {
                var typeConverter = _pipeline.FindComponentType<TypeConverter>();
                if (typeConverter != null)
                {
                    _typeConversionTable = typeConverter.TypeConversionTable;
                }
            }
            return _typeConversionTable;
        }
    }
}