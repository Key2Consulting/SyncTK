using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncTK.Internal
{
    public abstract class WriteFormatter : Component
    {
        protected TargetFile _target;
        protected ReadFormatter _formatter;
        protected IDataReader _reader = null;
        protected StreamWriter _writer = null;
        protected int _totalWriteCount = 0;
        protected int _fileWriteCount = 0;
        internal int _fileRowLimit = 0;

        internal WriteFormatter(int fileRowLimit = 0)
        {
            _fileRowLimit = fileRowLimit;
        }

        internal override IEnumerable<object> Process(IEnumerable<object> input)
        {
            // Unlike other pipeline components, converters must initialize/coordinate with the downstream
            // component to create the output stream e.g. files.
            _target = (TargetFile)GetDownstreamComponent();

            // Get our input explicitly via the input parameter, or implicitly via the upstream component.
            if (input == null)
            {
                // Upstream component must support the IDataReader interface.
                _reader = (IDataReader)GetUpstreamComponent();
                ProcessInput();
            }
            else
            {
                foreach (var i in input)
                {
                    // Input must support the IDataReader interface.
                    _reader = (IDataReader)i;
                    ProcessInput();
                }
            }

            return null;
        }

        protected void ProcessInput()
        {
            try
            {
                // Initial file. ProcessInput could be called again with another reader without
                // reaching the rollover limit. In which case we'd keep writing to same file.
                if (_writer == null)
                {
                    _writer = _target.GetNextStreamWriter();
                    OnBeginWriting();
                    OnBeginFile();
                }

                // For each data row
                while (_reader.Read())
                {
                    // If we've reached the maximum number of rows for the current file.
                    if (_fileWriteCount >= _fileRowLimit && _fileRowLimit > 0)
                    {
                        OnEndFile();
                        _fileWriteCount = 0;
                        _writer.Dispose();
                        _writer = _target.GetNextStreamWriter();
                        OnBeginFile();
                    }

                    OnWriteLine();
                    _totalWriteCount++;
                    _fileWriteCount++;
                }

                OnEndFile();
                _writer.Dispose();
                OnEndWriting();
            }
            catch (Exception ex)
            {
                throw new Exception($"Error writing line {_fileWriteCount}.", ex);
            }
        }

        protected virtual void OnBeginWriting()
        {
        }

        protected virtual void OnBeginFile()
        {
        }

        protected virtual void OnEndFile()
        {
        }

        protected virtual void OnEndWriting()
        {
        }

        protected abstract void OnWriteLine();
    }
}