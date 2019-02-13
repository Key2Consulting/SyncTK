using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage;
using System.IO;

namespace SyncTK
{
    public class TargetAzureBlob : ConnectorAzureBlob
    {
        protected int _fileRowLimit = 0;
        protected int _fileReadNumber = 0;
        protected int _fileWriteNumber = 0;
        protected int _rowCounter = 0;
        protected StreamWriter _streamWriter = null;

        public TargetAzureBlob(string connectionString, string containerName, string blobName, int fileRowLimit = 1000000)
        {
            _connectionString = connectionString;
            _containerName = containerName;
            _blobName = blobName;
            _fileRowLimit = fileRowLimit;
        }

        internal override void Validate(Sync pipeline, Component upstreamComponent)
        {
            this.Assert(_fileRowLimit == 0 || _blobName.Contains("*"), "Splitting output files requires use of wildcard character * in the blob path.");
        }

        internal override IEnumerable<object> Process(Sync pipeline, Component upstreamComponent, IEnumerable<object> input)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_connectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();

            _container = blobClient.GetContainerReference(_containerName);

            foreach (var i in input)
            {
                var writer = (IFileWriter)i;
                while (writer.Write(GetNextStreamWriter(), _fileReadNumber, _fileWriteNumber)) { }
                _fileReadNumber++;
            }

            // Targets don't produce output during processing.
            return null;
        }

        internal override void End(Sync pipeline, Component upstreamComponent)
        {
            // Dispose of last writer
            if (_streamWriter != null)
            {
                _streamWriter.Flush();
                _streamWriter.Close();
                _streamWriter.Dispose();
            }
        }

        protected StreamWriter GetNextStreamWriter()
        {
            _rowCounter++;

            // If we've met our split limit
            if (_rowCounter >= _fileRowLimit && _fileRowLimit > 0 || _streamWriter == null)
            {
                // Dispose of prior writer.
                if (_streamWriter != null)
                {
                    _streamWriter.Flush();
                    _streamWriter.Close();
                    _streamWriter.Dispose();
                }
                _fileWriteNumber++;
                var newBlobName = _blobName.Replace("*", this.GetCurrentTimeStampToken() + _fileWriteNumber.ToString());
                var blob = _container.GetBlockBlobReference(newBlobName);
                _streamWriter = new StreamWriter(blob.OpenWrite());
            }

            return _streamWriter;
        }
    }
}