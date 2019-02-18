using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using SyncTK.Internal;

namespace SyncTK
{
    public class SourceFile : Source
    {
        protected List<string> _paths = new List<string>();
        protected List<string> _rootPaths = new List<string>();
        protected List<string> _fileNames = new List<string>();
        protected List<StreamReader> _streamReaders = new List<StreamReader>();

        public SourceFile(string path)
        {
            _paths.Add(path);
        }

        public SourceFile(string[] paths)
        {
            _paths.AddRange(paths);
        }

        internal override void Validate()
        {
            // For every path we're loading.
            foreach (var path in _paths)
            {
                // Currently only support file wildcards, and not folder wildcards, so separate
                // out the file portion from the rest of the base path.
                var parts = path.Split(Path.DirectorySeparatorChar);
                string fileName = parts[parts.Length - 1];
                parts[parts.Length - 1] = "";
                var rootPath = string.Join(Path.DirectorySeparatorChar.ToString(), parts);
                   
                // Verify the root path doesn't contain wildcards.
                if (rootPath.Contains("*"))
                    throw new Exception("Folder wildcards are unsupported.");

                _rootPaths.Add(rootPath);
                _fileNames.Add(fileName);
            }

            Assert(GetDownstreamComponent() is ReadFormatter, "Pipeline is missing a ReadFormatter.");
        }

        internal override IEnumerable<object> Process(IEnumerable<object> input)
        {
            // For every path we're loading.
            for (int i = 0; i < _rootPaths.Count; i++)
            {
                // If the name contains wildcards.
                if (_fileNames[i].Contains("*"))
                {
                    var paths = Directory.GetFiles(_rootPaths[i], _fileNames[i]);

                    foreach (var item in paths)
                    {
                        var reader = new StreamReader(item);
                        yield return reader;
                    }
                }
                else
                {
                    // Else, just a single file.
                    var reader = new StreamReader(_rootPaths[i] + Path.DirectorySeparatorChar + _fileNames[i]);
                    _streamReaders.Add(reader);
                    yield return reader;
                }
            }
        }

        internal override void End()
        {
            foreach (var reader in _streamReaders)
            {
                reader.Close();
                reader.Dispose();
            }
        }
    }
}