using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage;
using System.IO;
using SyncTK.Internal;

namespace SyncTK
{
    public class SourceAzureBlob : Source
    {
        protected string _connectionString;
        protected string _blobName;
        protected string _containerName;
        protected CloudBlobContainer _container;

        protected List<StreamReader> _streamReaders = new List<StreamReader>();

        public SourceAzureBlob(string connectionString, string containerName, string blobName)
        {
            _connectionString = connectionString;
            _containerName = containerName;
            _blobName = blobName;
        }

        internal override void Validate(Pipeline pipeline)
        {
        }

        internal override IEnumerable<object> Process(Pipeline pipeline, IEnumerable<object> input)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_connectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer container = blobClient.GetContainerReference(_containerName);

            var directory = container.GetDirectoryReference(_blobName);
            foreach (CloudBlob item in directory.ListBlobs())
            {
                var blob = container.GetBlobReference(item.Name);
                var reader = new StreamReader(blob.OpenRead());
                _streamReaders.Add(reader);
                yield return reader;
            }
        }

        internal override void End(Pipeline pipeline)
        {
            foreach (var reader in _streamReaders)
            {
                reader.Close();
                reader.Dispose();
            }
        }
    }
}