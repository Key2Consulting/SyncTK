using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage;

namespace SyncTK.Pipeline.Target
{
    public class TargetAzureBlob : ConnectorAzureBlob
    {
        public TargetAzureBlob(string connectionString, string containerName, string blobName, bool createContainer = false)
        {
            _connectionString = connectionString;
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_connectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer container = null;
            if (createContainer)
            {
                container = new CloudBlobContainer(new Uri("containerAddress"));
                container.Create();
            }
            else
            {
                container = blobClient.GetContainerReference(containerName);
            }

            // container.ListBlobs().ToList().select

            var blob = container.GetBlockBlobReference("blobName");
            var blob2 = new CloudBlockBlob(new Uri("blobAbsoluteUri"));

            var stream = blob.OpenRead();
        }

        public TargetAzureBlob(string blobAbsoluteUri)
        {
            var container = new CloudBlobContainer(new Uri("containerAddress"));
            container.Create();
            // container.ListBlobs().ToList().select

            var blob = container.GetBlockBlobReference("blobName");
            var blob2 = new CloudBlockBlob(new Uri("blobAbsoluteUri"));
            
            var stream = blob.OpenRead();
        }

        public TargetAzureBlob(string accountName, string keyValue, string container, string blobName, bool createContainer = false, bool useHTTPS = true)
        {
            var storageCredentials = new StorageCredentials(accountName, keyValue);
            var storageAccount = new CloudStorageAccount(storageCredentials, useHTTPS);

            var stream = storageAccount.CreateCloudBlobClient().GetContainerReference(container).GetBlobReference(blobName).OpenRead();
        }
    }
}