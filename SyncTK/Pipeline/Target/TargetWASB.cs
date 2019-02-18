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
    public class TargetWASB : TargetFile
    {
        protected string _connectionString;
        protected string _blobName;
        protected string _containerName;
        protected CloudBlobContainer _container;

        protected int _fileRowLimit = 0;
        protected int _fileReadNumber = 0;
        protected int _fileWriteNumber = 0;
        protected int _rowCounter = 0;

        public TargetWASB(string connectionString, string containerName, string blobName) : base(blobName)
        {
            _connectionString = connectionString;
            _containerName = containerName;
            _blobName = blobName;
        }

        internal override StreamWriter GetNextStreamWriter()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_connectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();

            _container = blobClient.GetContainerReference(_containerName);

            var newBlobName = _blobName.Replace("*", this.GetCurrentTimeStampToken() + _fileWriteNumber.ToString());
            var blob = _container.GetBlockBlobReference(newBlobName);
            _outputFiles.Add(blob.Uri.ToString());
            _streamWriter = new StreamWriter(blob.OpenWrite());
        
            return _streamWriter;
        }
    }
}