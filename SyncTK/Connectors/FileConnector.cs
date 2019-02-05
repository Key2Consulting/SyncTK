using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;

namespace SyncTK
{
    public class FileConnector : Connector
    {
        public FileConnector()
        {
        }

        public virtual IEnumerable<StreamReader> Export(string path)
        {
            // If the path contains wildcards.
            if (path.Contains("*"))
            {
                // Currently only support file wildcards, and no folder wildcards, so separate
                // out the file portion from the rest of the base path.
                var parts = path.Split(Path.DirectorySeparatorChar);
                string searchPattern = parts[parts.Length - 1];
                parts[parts.Length - 1] = "";
                var rootPath = string.Join(Path.DirectorySeparatorChar, parts);

                if (rootPath.Contains("*"))
                    throw new Exception("Folder wildcards are unsupported.");
                var paths = Directory.GetFiles(rootPath, searchPattern);

                foreach (var item in paths)
                {
                    var reader = new StreamReader(item);
                    yield return reader;
                }
            }
            else
            {
                // Else, just a single file.
                var reader = new StreamReader(path);
                yield return reader;
            }
        }

        public virtual IEnumerable<StreamReader> Export(string[] paths)
        {
            List<StreamReader> readers = new List<StreamReader>();
            foreach (string path in paths)
            {
                var reader = new StreamReader(path);
                yield return reader;
            }
        }

        protected string [] FindFiles(string rootPath, string searchPattern)
        {
            return null;
        }
    }
}