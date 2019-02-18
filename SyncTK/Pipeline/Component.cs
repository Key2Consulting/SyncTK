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

        internal virtual void Begin()
        {
        }

        internal virtual void End()
        {
        }

        internal virtual IEnumerable<object> Process(IEnumerable<object> input)
        {
            throw new NotImplementedException();
        }

        internal virtual void Validate()
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

        protected Component GetUpstreamComponent()
        {
            var currentIndex = _pipeline._component.IndexOf(this);
            if (currentIndex > 0)
                return _pipeline._component[currentIndex - 1];
            return null;
        }

        protected Component GetDownstreamComponent()
        {
            var currentIndex = _pipeline._component.IndexOf(this);
            if (currentIndex < _pipeline._component.Count - 1)
                return _pipeline._component[currentIndex + 1];
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