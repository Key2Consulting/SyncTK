using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncTK
{
    public class ConnectorAzureBlob : Component
    {
        protected string _connectionString;
        protected string _blobName;
        protected string _containerName;
    }
}