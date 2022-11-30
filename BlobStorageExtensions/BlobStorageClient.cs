using Azure.Core.Pipeline;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http.Headers;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BlobStorageExtensions
{
    public class BlobStorageClient
    {


        private BlobServiceClient blobServiceClient;

        private bool autoCreateContainers = false;

        #region Constructors

        protected BlobStorageClient(bool autoCreateContainers = false)
        {
            this.autoCreateContainers = autoCreateContainers;
        }

        public BlobStorageClient(string connectionString, bool autoCreateContainers = false)
        {
            blobServiceClient = new BlobServiceClient(connectionString);
            this.autoCreateContainers = autoCreateContainers;
        }

        public BlobStorageClient(string connectionString, BlobClientOptions options, bool autoCreateContainers = false)
        {
            blobServiceClient = new BlobServiceClient(connectionString, options);
            this.autoCreateContainers = autoCreateContainers;
        }


        public BlobStorageClient(Uri serviceUri, BlobClientOptions options = default, bool autoCreateContainers = false)
        {
            blobServiceClient = new BlobServiceClient(serviceUri, options);
            this.autoCreateContainers = autoCreateContainers;
        }

        public BlobStorageClient(Uri serviceUri, Azure.AzureSasCredential credential, BlobClientOptions options = default, bool autoCreateContainers = false)
        {
            blobServiceClient = new BlobServiceClient(serviceUri, credential, options);
            this.autoCreateContainers = autoCreateContainers;
        }


        public BlobStorageClient(Uri serviceUri, Azure.Core.TokenCredential credential, BlobClientOptions options = default, bool autoCreateContainers = false)
        {
            blobServiceClient = new BlobServiceClient(serviceUri, credential, options);
            this.autoCreateContainers = autoCreateContainers;
        }


        public BlobStorageClient(Uri serviceUri, Azure.Storage.StorageSharedKeyCredential credential, BlobClientOptions options = default, bool autoCreateContainers = false)
        {
            blobServiceClient = new BlobServiceClient(serviceUri, credential, options);
            this.autoCreateContainers = autoCreateContainers;
        }


        #endregion


        #region Properties

        public string AccountName
        {
            get { return blobServiceClient.AccountName; }
        }

        public virtual bool CanGenerateAccountSasUri
        {
            get { return blobServiceClient.CanGenerateAccountSasUri; }
        }

        public virtual Uri Uri
        {
            get { return blobServiceClient.Uri; }
        }

        #endregion


        // Notes
        // -- When you create a BlobContainer, it is returning the BlobContainerClient for the container.  This seems not to apply so much any more
        // -- Eliminated GetBlobContainerClient - The whole point is to have one client?  But Maybe it should stay?
        // -- Things like GetProperties, SetProperties, will we need to rename to add in Account, Container, Blob???

        #region Container Methods

        public virtual Azure.Response<BlobContainerInfo> CreateBlobContainer(string blobContainerName,
            PublicAccessType publicAccessType = PublicAccessType.None, IDictionary<string, string> metadata = default,
            BlobContainerEncryptionScopeOptions encryptionScopeOptions = default, CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return blobContainerClient.Create(publicAccessType, metadata, encryptionScopeOptions, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobContainerInfo>> CreateBlobContainerAsync(string blobContainerName,
            PublicAccessType publicAccessType = PublicAccessType.None, IDictionary<string, string> metadata = default,
            BlobContainerEncryptionScopeOptions encryptionScopeOptions = default, CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return await blobContainerClient.CreateAsync(publicAccessType, metadata, encryptionScopeOptions, cancellationToken);
        }


        public virtual Azure.Response<BlobContainerInfo> CreateBlobContainerIfNotExists(string blobContainerName,
            PublicAccessType publicAccessType = PublicAccessType.None, IDictionary<string, string> metadata = default,
            BlobContainerEncryptionScopeOptions encryptionScopeOptions = default, CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return blobContainerClient.CreateIfNotExists(publicAccessType, metadata, encryptionScopeOptions, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobContainerInfo>> CreateBlobContainerIfNotExistsAsync(string blobContainerName,
            PublicAccessType publicAccessType = PublicAccessType.None, IDictionary<string, string> metadata = default,
            BlobContainerEncryptionScopeOptions encryptionScopeOptions = default, CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return await blobContainerClient.CreateIfNotExistsAsync(publicAccessType, metadata, encryptionScopeOptions, cancellationToken);
        }


        public virtual Azure.Response DeleteBlobContainer(string blobContainerName,
            BlobRequestConditions conditions = default, CancellationToken cancellationToken = default)
        {
            return blobServiceClient.DeleteBlobContainer(blobContainerName, conditions, cancellationToken);
        }


        public virtual async Task<Azure.Response> DeleteBlobContainerAsync(string blobContainerName, BlobRequestConditions conditions = default,
            CancellationToken cancellationToken = default)
        {
            return await blobServiceClient.DeleteBlobContainerAsync(blobContainerName, conditions, cancellationToken);
        }

        public virtual Azure.Response<Azure.Storage.Blobs.BlobContainerClient> UndeleteBlobContainer(string deletedContainerName, string deletedContainerVersion, CancellationToken cancellationToken = default)
        {
            return blobServiceClient.UndeleteBlobContainer(deletedContainerName, deletedContainerVersion, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobContainerClient>> UndeleteBlobContainerAsync(string deletedContainerName, string deletedContainerVersion, CancellationToken cancellationToken = default)
        {
            return await blobServiceClient.UndeleteBlobContainerAsync(deletedContainerName, deletedContainerVersion, cancellationToken);
        }



        public virtual Azure.Response<bool> ContainerExists(string blobContainerName, CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return blobContainerClient.Exists(cancellationToken);
        }


        public virtual async Task<Azure.Response<bool>> ContainerExistsAsync(string blobContainerName, CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return await blobContainerClient.ExistsAsync(cancellationToken);
        }


        public virtual Azure.Response<BlobContainerInfo> SetContainerMetadata(string blobContainerName, IDictionary<string, string> metadata,
            BlobRequestConditions conditions = default, CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return blobContainerClient.SetMetadata(metadata, conditions, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobContainerInfo>> SetContainerMetadataAsync(string blobContainerName, IDictionary<string, string> metadata,
            BlobRequestConditions conditions = default, CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return await blobContainerClient.SetMetadataAsync(metadata, conditions, cancellationToken);
        }



        #endregion




        public virtual Azure.Pageable<BlobItem> GetBlobs(string blobContainerName, BlobTraits traits = BlobTraits.None, BlobStates states = BlobStates.None,
            string prefix = default, CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return blobContainerClient.GetBlobs(traits, states, prefix, cancellationToken);
        }


        public virtual Azure.AsyncPageable<BlobItem> GetBlobsAsync(string blobContainerName, BlobTraits traits = BlobTraits.None,
            BlobStates states = BlobStates.None, string prefix = default, CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return blobContainerClient.GetBlobsAsync(traits, states, prefix, cancellationToken);
        }


        public virtual Azure.Pageable<BlobHierarchyItem> GetBlobsByHierarchy(string blobContainerName, BlobTraits traits = BlobTraits.None,
            BlobStates states = BlobStates.None, string delimiter = default, string prefix = default, CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return blobContainerClient.GetBlobsByHierarchy(traits, states, delimiter, prefix, cancellationToken);
        }


        public virtual Azure.AsyncPageable<BlobHierarchyItem> GetBlobsByHierarchyAsync(string blobContainerName, BlobTraits traits = BlobTraits.None,
            BlobStates states = BlobStates.None, string delimiter = default, string prefix = default, CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return blobContainerClient.GetBlobsByHierarchyAsync(traits, states, delimiter, prefix, cancellationToken);
        }








        #region Upload From File - Sync


        public virtual Azure.Response<BlobContentInfo> UploadFromFile(string blobContainerName, string blobName, string filePath)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return blobClient.Upload(filePath);
        }


        public virtual Azure.Response<BlobContentInfo> UploadFromFile(string blobContainerName, string blobName, string filePath, CancellationToken cancellationToken)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return blobClient.Upload(filePath, cancellationToken);
        }


        public virtual Azure.Response<BlobContentInfo> UploadFromFile(string blobContainerName, string blobName, string filePath, bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return blobClient.Upload(filePath, overwrite, cancellationToken);
        }


        public virtual Azure.Response<BlobContentInfo> UploadFromFile(string blobContainerName, string blobName, string filePath, BlobUploadOptions options,
            CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return blobClient.Upload(filePath, options, cancellationToken);
        }





        // ADDED BY DAVID

        public virtual Azure.Response<BlobContentInfo> UploadFromFile(string blobContainerName, string blobName, string filePath, string contentType,
            bool overwrite = false, CancellationToken cancellationToken = default)
        {
            // Check if the container exists and create it if it doesn't
            if (autoCreateContainers)
            {
                var containerClient = GetBlobContainerClient(blobContainerName);
                containerClient.CreateIfNotExists();
            }

            // Now upload the blob
            var blobClient = GetBlobClient(blobContainerName, blobName);
            var options = new BlobUploadOptions() { HttpHeaders = new BlobHttpHeaders() { ContentType = contentType } };

            return blobClient.Upload(filePath, options, cancellationToken);
        }

        // END ADDED BY DAVID


        #endregion


        #region Upload From File - Async

        public virtual async Task<Azure.Response<BlobContentInfo>> UploadAsync(string blobContainerName, string blobName, string filePath)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return await blobClient.UploadAsync(filePath);
        }

        public virtual async Task<Azure.Response<BlobContentInfo>> UploadAsync(string blobContainerName, string blobName, string filePath,
            CancellationToken cancellationToken)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return await blobClient.UploadAsync(filePath, cancellationToken);
        }

        public virtual async Task<Azure.Response<BlobContentInfo>> UploadAsync(string blobContainerName, string blobName, string filePath,
            bool overwrite = false, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return await blobClient.UploadAsync(filePath, overwrite, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobContentInfo>> UploadAsync(string blobContainerName, string blobName, string filePath,
            BlobUploadOptions options, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return await blobClient.UploadAsync(filePath, options, cancellationToken);
        }


        #endregion


        #region Upload From Stream - Sync

        public virtual Azure.Response<BlobContentInfo> UploadFromStream(string blobContainerName, string blobName, Stream content)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return blobClient.Upload(content);
        }

        public virtual Azure.Response<BlobContentInfo> UploadFromStream(string blobContainerName, string blobName, Stream content, 
            CancellationToken cancellationToken)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return blobClient.Upload(content, cancellationToken);
        }


        public virtual Azure.Response<BlobContentInfo> UploadFromStream(string blobContainerName, string blobName, Stream content, bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return blobClient.Upload(content, overwrite, cancellationToken);
        }



        public virtual Azure.Response<BlobContentInfo> UploadFromStream(string blobContainerName, string blobName, Stream content, BlobUploadOptions options,
            CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return blobClient.Upload(content, options, cancellationToken);
        }

        #endregion


        #region Upload from Stream - Async

        public virtual async Task<Azure.Response<BlobContentInfo>> UploadFromStreamAsync(string blobContainerName, string blobName, Stream content)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return await blobClient.UploadAsync(content);
        }


        public virtual async Task<Azure.Response<BlobContentInfo>> UploadFromStreamAsync(string blobContainerName, string blobName, Stream content,
            CancellationToken cancellationToken)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return await blobClient.UploadAsync(content, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobContentInfo>> UploadFromStreamAsync(string blobContainerName, string blobName, Stream content,
            bool overwrite = false, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return await blobClient.UploadAsync(content, overwrite, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobContentInfo>> UploadFromStreamAsync(string blobContainerName, string blobName, Stream content,
            BlobUploadOptions options, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return await blobClient.UploadAsync(content, options, cancellationToken);
        }

        #endregion


        #region Upload From Binary Data - Sync


        public virtual Azure.Response<BlobContentInfo> UploadFromBinaryContent(string blobContainerName, string blobName, BinaryData content)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return blobClient.Upload(content);
        }


        public virtual Azure.Response<BlobContentInfo> UploadFromBinaryContent(string blobContainerName, string blobName, BinaryData content,
            CancellationToken cancellationToken)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return blobClient.Upload(content, cancellationToken);
        }


        public virtual Azure.Response<BlobContentInfo> UploadFromBinaryContent(string blobContainerName, string blobName, BinaryData content, 
            bool overwrite = false, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return blobClient.Upload(content, overwrite, cancellationToken);
        }


        public virtual Azure.Response<BlobContentInfo> UploadFromBinaryContent(string blobContainerName, string blobName, BinaryData content, 
            BlobUploadOptions options, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return blobClient.Upload(content, options, cancellationToken);
        }


        #endregion


        #region Upload from Binary Data - Async

        public virtual async Task<Azure.Response<BlobContentInfo>> UploadFromBinaryContentAsync(string blobContainerName, string blobName, BinaryData content)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return await blobClient.UploadAsync(content);
        }

        public virtual async Task<Azure.Response<BlobContentInfo>> UploadFromBinaryContentAsync(string blobContainerName, string blobName, BinaryData content,
            CancellationToken cancellationToken)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return await blobClient.UploadAsync(content, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobContentInfo>> UploadFromBinaryContentAsync(string blobContainerName, string blobName, BinaryData content,
            bool overwrite = false, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return await blobClient.UploadAsync(content, overwrite, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobContentInfo>> UploadBinaryContentAsync(string blobContainerName, string blobName, BinaryData content,
            BlobUploadOptions options, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return await blobClient.UploadAsync(content, options, cancellationToken);
        }

        #endregion


        #region Open Write Methods

        public virtual Stream OpenWrite(string blobContainerName, string blobName, bool overwrite, BlobOpenWriteOptions options = default,
            CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            return blobClient.OpenWrite(overwrite, options, cancellationToken);
        }




        public virtual async Task<Stream> OpenWriteAsync(string blobContainerName, string blobName, bool overwrite, BlobOpenWriteOptions options = default,
            CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            return await blobClient.OpenWriteAsync(overwrite, options, cancellationToken);
        }

        #endregion


        #region Delete Blob

        public virtual Azure.Response DeleteBlob(string blobContainerName, string blobName, DeleteSnapshotsOption snapshotsOption = DeleteSnapshotsOption.None,
            BlobRequestConditions conditions = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return blobClient.Delete(snapshotsOption, conditions, cancellationToken);
        }

        public virtual async Task<Azure.Response> DeleteBlobAsync(string blobContainerName, string blobName,
            DeleteSnapshotsOption snapshotsOption = DeleteSnapshotsOption.None, BlobRequestConditions conditions = default,
            CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return await blobClient.DeleteAsync(snapshotsOption, conditions, cancellationToken);
        }


        public virtual Azure.Response<bool> DeleteBlobIfExists(string blobContainerName, string blobName,
            DeleteSnapshotsOption snapshotsOption = DeleteSnapshotsOption.None, BlobRequestConditions conditions = default,
            CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return blobClient.DeleteIfExists(snapshotsOption, conditions, cancellationToken);
        }


        public virtual async Task<Azure.Response<bool>> DeleteBlobIfExistsAsync(string blobContainerName, string blobName,
            DeleteSnapshotsOption snapshotsOption = DeleteSnapshotsOption.None, BlobRequestConditions conditions = default,
            CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return await blobClient.DeleteIfExistsAsync(snapshotsOption, conditions, cancellationToken);
        }


        public virtual Azure.Response UndeleteBlob(string blobContainerName, string blobName, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return blobClient.Undelete(cancellationToken);
        }


        public virtual async Task<Azure.Response> UndeleteAsync(string blobContainerName, string blobName, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return await blobClient.UndeleteAsync(cancellationToken);
        }


        #endregion


        #region Blob Exists

        public virtual Azure.Response<bool> Exists(string blobContainerName, string blobName, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.Exists(cancellationToken);
        }

        public virtual async Task<Azure.Response<bool>> ExistsAsync(string blobContainerName, string blobName, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.ExistsAsync(cancellationToken);
        }


        #endregion


        #region Download Blob - Binary Data - Sync

        // ----------------------------------------------------------------------------------------
        // CHANGE - All of these methods were called DownloadContent().  But they don't just download the content,
        // they get an object that has the properties and the content of the blob.  So it feels like they should be DownloadBlob()
        // ----------------------------------------------------------------------------------------

        /// <summary>
        /// Returns a BlobDownloadResult object that contains both the properties and content of the blob, with the the content of the blob
        /// represented as a BinaryData object.
        /// </summary>
        /// <param name="blobContainerName"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        public virtual Azure.Response<BlobDownloadResult> DownloadBlob(string blobContainerName, string blobName)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.DownloadContent();
        }

        public virtual Azure.Response<BlobDownloadResult> DownloadBlob(string blobContainerName, string blobName,
            CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.DownloadContent(cancellationToken);
        }

        public virtual Azure.Response<BlobDownloadResult> DownloadBlob(string blobContainerName, string blobName,
            BlobDownloadOptions options = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.DownloadContent(options, cancellationToken);
        }

        #endregion


        #region Download Blob - BinaryData - Async

        public virtual async Task<Azure.Response<BlobDownloadResult>> DownloadBlobAsync(string blobContainerName, string blobName)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.DownloadContentAsync();
        }

        public virtual async Task<Azure.Response<BlobDownloadResult>> DownloadBlobAsync(string blobContainerName, string blobName,
            CancellationToken cancellationToken)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.DownloadContentAsync(cancellationToken);
        }

        public virtual async Task<Azure.Response<BlobDownloadResult>> DownloadBlobAsync(string blobContainerName, string blobName,
            BlobDownloadOptions options = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.DownloadContentAsync(options, cancellationToken);
        }

        #endregion


        public virtual Azure.Response<BlobDownloadStreamingResult> DownloadBlobStreaming(string blobContainerName, string blobName,
            BlobDownloadOptions options = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.DownloadStreaming(options, cancellationToken);
        }

        public virtual async Task<Azure.Response<BlobDownloadStreamingResult>> DownloadBlobStreamingAsync(string blobContainerName, string blobName,
            BlobDownloadOptions options = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.DownloadStreamingAsync(options, cancellationToken);
        }



        #region Download Blob To File - Sync

        public virtual Azure.Response DownloadBlobToFile(string blobContainerName, string blobName, string path)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.DownloadTo(path);
        }

        public virtual Azure.Response DownloadBlobToFile(string blobContainerName, string blobName, string path, CancellationToken cancellationToken)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.DownloadTo(path, cancellationToken);
        }

        public virtual Azure.Response DownloadBlobToFile(string blobContainerName, string blobName, string path, BlobDownloadToOptions options,
            CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.DownloadTo(path, options, cancellationToken);
        }

        #endregion


        #region Download Blob to File - Async

        public virtual async Task<Azure.Response> DownloadBlobToFileAsync(string blobContainerName, string blobName, string path)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.DownloadToAsync(path);
        }

        public virtual async Task<Azure.Response> DownloadBlobToFileAsync(string blobContainerName, string blobName, string path, CancellationToken cancellationToken)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.DownloadToAsync(path, cancellationToken);
        }

        public virtual async Task<Azure.Response> DownloadBlobToFileAsync(string blobContainerName, string blobName, string path,
            BlobDownloadToOptions options, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.DownloadToAsync(path, options, cancellationToken);
        }

        #endregion



        #region Download Blob Contents to Stream

        public virtual Azure.Response DownloadBlobToStream(string blobContainerName, string blobName, Stream destination)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.DownloadTo(destination);
        }

        public virtual Azure.Response DownloadBlobToStream(string blobContainerName, string blobName, Stream destination, CancellationToken cancellationToken)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.DownloadTo(destination, cancellationToken);
        }


        public virtual Azure.Response DownloadBlobToStream(string blobContainerName, string blobName, Stream destination, BlobDownloadToOptions options,
            CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.DownloadTo(destination, options, cancellationToken);
        }

        #endregion


        #region Download Blob Contents to Stream

        public virtual async Task<Azure.Response> DownloadBlobToStreamAsync(string blobContainerName, string blobName, Stream destination)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.DownloadToAsync(destination);
        }

        public virtual async Task<Azure.Response> DownloadBlobToStreamAsync(string blobContainerName, string blobName, Stream destination, CancellationToken cancellationToken)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.DownloadToAsync(destination, cancellationToken);
        }

        public virtual async Task<Azure.Response> DownloadBlobToStreamAsync(string blobContainerName, string blobName, Stream destination,
            BlobDownloadToOptions options, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.DownloadToAsync(destination, options, cancellationToken);
        }

        #endregion


        #region Open Read

        public virtual Stream OpenRead(string blobContainerName, string blobName, BlobOpenReadOptions options, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.OpenRead(options, cancellationToken);
        }

        public virtual async Task<Stream> OpenReadAsync(string blobContainerName, string blobName, BlobOpenReadOptions options, 
            CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.OpenReadAsync(options, cancellationToken);
        }

        #endregion


        #region Blob Immutability

        public virtual Azure.Response DeleteImmutabilityPolicy(string blobContainerName, string blobName, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.DeleteImmutabilityPolicy(cancellationToken);
        }


        public virtual async Task<Azure.Response> DeleteImmutabilityPolicyAsync(string blobContainerName, string blobName,
            CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.DeleteImmutabilityPolicyAsync(cancellationToken);
        }


        public virtual Azure.Response<BlobImmutabilityPolicy> SetImmutabilityPolicy(string blobContainerName, string blobName,
            BlobImmutabilityPolicy immutabilityPolicy, BlobRequestConditions conditions = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return blobClient.SetImmutabilityPolicy(immutabilityPolicy, conditions, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobImmutabilityPolicy>> SetImmutabilityPolicyAsync(string blobContainerName, string blobName,
            BlobImmutabilityPolicy immutabilityPolicy, BlobRequestConditions conditions = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return await blobClient.SetImmutabilityPolicyAsync(immutabilityPolicy, conditions, cancellationToken);
        }

        #endregion


        #region Blob Snaphots

        public virtual Azure.Response<BlobSnapshotInfo> CreateBlobSnapshot(string blobContainerName, string blobName,
            IDictionary<string, string> metadata = default, BlobRequestConditions conditions = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.CreateSnapshot(metadata, conditions, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobSnapshotInfo>> CreateBlobSnapshotAsync(string blobContainerName, string blobName,
            IDictionary<string, string> metadata = default, BlobRequestConditions conditions = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.CreateSnapshotAsync(metadata, conditions, cancellationToken);
        }

        #endregion


        #region Blob SAS Uri

        public virtual Uri GenerateBlobSasUri(string blobContainerName, string blobName, BlobSasBuilder builder)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.GenerateSasUri(builder);
        }


        public virtual Uri GenerateBlobSasUri(string blobContainerName, string blobName, BlobSasPermissions permissions, DateTimeOffset expiresOn)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.GenerateSasUri(permissions, expiresOn);
        }

        #endregion


        #region Blob Properties

        public virtual Azure.Response<BlobProperties> GetBlobProperties(string blobContainerName, string blobName, BlobRequestConditions conditions = default,
            CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.GetProperties(conditions, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobProperties>> GetBlobPropertiesAsync(string blobContainerName, string blobName,
            BlobRequestConditions conditions = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.GetPropertiesAsync(conditions, cancellationToken);
        }


        #endregion


        #region Blob Tags

        public virtual Azure.Response<GetBlobTagResult> GetBlobTags(string blobContainerName, string blobName, BlobRequestConditions conditions = default, 
            CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.GetTags(conditions, cancellationToken);

        }

        public virtual async Task<Azure.Response<GetBlobTagResult>> GetBlobTagsAsync(string blobContainerName, string blobName, 
            BlobRequestConditions conditions = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.GetTagsAsync(conditions, cancellationToken);
        }


        public virtual Azure.Response SetBlobTags(string blobContainerName, string blobName, IDictionary<string, string> tags, 
            BlobRequestConditions conditions = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return blobClient.SetTags(tags, conditions, cancellationToken);
        }

        public virtual async Task<Azure.Response> SetTagsAsync(string blobContainerName, string blobName, IDictionary<string, string> tags, BlobRequestConditions conditions = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobClient(blobContainerName, blobName);
            return await blobClient.SetTagsAsync(tags, conditions, cancellationToken);
        }


        #endregion


        #region Blob Access Tier

        public virtual Azure.Response SetBlobAccessTier(string blobContainerName, string blobName, AccessTier accessTier, 
            BlobRequestConditions conditions = default, RehydratePriority? rehydratePriority = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.SetAccessTier(accessTier, conditions, rehydratePriority, cancellationToken);
        }


        public virtual async Task<Azure.Response> SetBlobAccessTierAsync(string blobContainerName, string blobName, AccessTier accessTier, 
            BlobRequestConditions conditions = default, RehydratePriority? rehydratePriority = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.SetAccessTierAsync(accessTier, conditions, rehydratePriority, cancellationToken);
        }


        #endregion


        #region Blob HTTP Headers

        public virtual Azure.Response<BlobInfo> SetHttpHeaders(string blobContainerName, string blobName, BlobHttpHeaders httpHeaders = default,
            BlobRequestConditions conditions = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.SetHttpHeaders(httpHeaders, conditions, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobInfo>> SetHttpHeadersAsync(string blobContainerName, string blobName, BlobHttpHeaders httpHeaders = default,
            BlobRequestConditions conditions = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.SetHttpHeadersAsync(httpHeaders, conditions, cancellationToken);
        }

        #endregion


        #region Blob Legal Hold

        public virtual Azure.Response<BlobLegalHoldResult> SetBlobLegalHold(string blobContainerName, string blobName, bool hasLegalHold, 
            CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.SetLegalHold(hasLegalHold, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobLegalHoldResult>> SetBlobLegalHoldAsync(string blobContainerName, string blobName, bool hasLegalHold, 
            CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.SetLegalHoldAsync(hasLegalHold, cancellationToken);
        }

        #endregion


        #region Blob Metadata

        public virtual Azure.Response<BlobInfo> SetBlobMetadata(string blobContainerName, string blobName, IDictionary<string, string> metadata, 
            BlobRequestConditions conditions = default, CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return blobClient.SetMetadata(metadata, conditions, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobInfo>> SetMetadataAsync(string blobContainerName, string blobName, IDictionary<string, string> metadata, 
            BlobRequestConditions conditions = default, System.Threading.CancellationToken cancellationToken = default)
        {
            var blobClient = GetBlobBaseClientCore(blobContainerName, blobName);
            return await blobClient.SetMetadataAsync(metadata, conditions, cancellationToken);
        }


        #endregion




        public virtual Azure.Pageable<TaggedBlobItem> FindBlobsByTags(string tagFilterSqlExpression, CancellationToken cancellationToken = default)
        {
            return blobServiceClient.FindBlobsByTags(tagFilterSqlExpression, cancellationToken);
        }

        public virtual Azure.AsyncPageable<TaggedBlobItem> FindBlobsByTagsAsync(string tagFilterSqlExpression, CancellationToken cancellationToken = default)
        {
            return blobServiceClient.FindBlobsByTagsAsync(tagFilterSqlExpression, cancellationToken);
        }


        public virtual Azure.Pageable<TaggedBlobItem> FindBlobsByTags(string blobContainerName, string tagFilterSqlExpression, CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return blobContainerClient.FindBlobsByTags(tagFilterSqlExpression, cancellationToken);
        }


        public virtual Azure.AsyncPageable<TaggedBlobItem> FindBlobsByTagsAsync(string blobContainerName, string tagFilterSqlExpression, System.Threading.CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return blobContainerClient.FindBlobsByTagsAsync(tagFilterSqlExpression, cancellationToken);
        }





        public Uri GenerateAccountSasUri(AccountSasBuilder builder)
        {
            return blobServiceClient.GenerateAccountSasUri(builder);
        }


        public Uri GenerateAccountSasUri(AccountSasPermissions permissions, DateTimeOffset expiresOn, AccountSasResourceTypes resourceTypes)
        {
            return blobServiceClient.GenerateAccountSasUri(permissions, expiresOn, resourceTypes);
        }


        public virtual Uri GenerateContainerSasUri(string blobContainerName, BlobSasBuilder builder)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return blobContainerClient.GenerateSasUri(builder);
        }

        public virtual Uri GenerateContainerSasUri(string blobContainerName, BlobContainerSasPermissions permissions, DateTimeOffset expiresOn)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return blobContainerClient.GenerateSasUri(permissions, expiresOn);
        }



        public virtual Azure.Response<AccountInfo> GetAccountInfo(CancellationToken cancellationToken = default)
        {
            return blobServiceClient.GetAccountInfo(cancellationToken);
        }


        public virtual async Task<Azure.Response<AccountInfo>> GetAccountInfoAsync(CancellationToken cancellationToken = default)
        {
            return await blobServiceClient.GetAccountInfoAsync(cancellationToken);
        }




        #region Access Policies - Containers

        public virtual Azure.Response<BlobContainerAccessPolicy> GetContainerAccessPolicy(string blobContainerName, BlobRequestConditions conditions = default, 
            CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return blobContainerClient.GetAccessPolicy(conditions, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobContainerAccessPolicy>> GetContainerAccessPolicyAsync(string blobContainerName, BlobRequestConditions conditions = default, 
            CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return await blobContainerClient.GetAccessPolicyAsync(conditions, cancellationToken);

        }


        public virtual Azure.Response<BlobContainerInfo> SetContainerAccessPolicy(string blobContainerName, PublicAccessType accessType = PublicAccessType.None, 
            IEnumerable<BlobSignedIdentifier> permissions = default, BlobRequestConditions conditions = default, CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return blobContainerClient.SetAccessPolicy(accessType, permissions, conditions, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobContainerInfo>> SetContainerAccessPolicyAsync(string blobContainerName, PublicAccessType accessType = PublicAccessType.None,
            IEnumerable<BlobSignedIdentifier> permissions = default, BlobRequestConditions conditions = default, CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return await blobContainerClient.SetAccessPolicyAsync(accessType, permissions, conditions, cancellationToken);
        }


        #endregion




        public virtual Azure.Pageable<BlobContainerItem> GetBlobContainers(BlobContainerTraits traits = BlobContainerTraits.None, 
            BlobContainerStates states = BlobContainerStates.None, string prefix = default, CancellationToken cancellationToken = default)
        {
            return blobServiceClient.GetBlobContainers(traits, states, prefix, cancellationToken);
        }


        public virtual Azure.AsyncPageable<BlobContainerItem> GetBlobContainersAsync(BlobContainerTraits traits = BlobContainerTraits.None, 
            BlobContainerStates states = BlobContainerStates.None, string prefix = default, CancellationToken cancellationToken = default)
        {
            return blobServiceClient.GetBlobContainersAsync(traits, states, prefix, cancellationToken);
        }


        public virtual Azure.Response<BlobServiceProperties> GetAccountProperties(CancellationToken cancellationToken = default)
        {
            return blobServiceClient.GetProperties(cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobServiceProperties>> GetAccountPropertiesAsync(CancellationToken cancellationToken = default)
        {
            return await blobServiceClient.GetPropertiesAsync(cancellationToken);
        }



        public virtual Azure.Response<BlobContainerProperties> GetContainerProperties(string blobContainerName, BlobRequestConditions conditions = default, 
            CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return blobContainerClient.GetProperties(conditions, cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobContainerProperties>> GetPropertiesAsync(string blobContainerName, BlobRequestConditions conditions = default, 
            CancellationToken cancellationToken = default)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return await blobContainerClient.GetPropertiesAsync(conditions, cancellationToken);
        }





        public virtual Azure.Response<BlobServiceStatistics> GetAccountStatistics(CancellationToken cancellationToken = default)
        {
            return blobServiceClient.GetStatistics(cancellationToken);
        }


        public virtual async Task<Azure.Response<BlobServiceStatistics>> GetAccountStatisticsAsync(CancellationToken cancellationToken = default)
        {
            return await blobServiceClient.GetStatisticsAsync(cancellationToken);
        }


        public virtual Azure.Response<UserDelegationKey> GetUserDelegationKey(DateTimeOffset? startsOn, DateTimeOffset expiresOn, CancellationToken cancellationToken = default)
        {
            return blobServiceClient.GetUserDelegationKey(startsOn, expiresOn, cancellationToken);
        }


        public virtual async Task<Azure.Response<UserDelegationKey>> GetUserDelegationKeyAsync(DateTimeOffset? startsOn, DateTimeOffset expiresOn, CancellationToken cancellationToken = default)
        {
            return await blobServiceClient.GetUserDelegationKeyAsync(startsOn, expiresOn, cancellationToken);
        }






        public virtual Azure.Response SetAccountProperties(BlobServiceProperties properties, CancellationToken cancellationToken = default)
        {
            return blobServiceClient.SetProperties(properties, cancellationToken);
        }


        public virtual async Task<Azure.Response> SetAccountPropertiesAsync(BlobServiceProperties properties, CancellationToken cancellationToken = default)
        {
            return await blobServiceClient.SetPropertiesAsync(properties, cancellationToken);
        }






        #region Client Getters



        protected internal virtual Azure.Storage.Blobs.Specialized.BlobBaseClient GetBlobBaseClientCore(string blobContainerName, string blobName)
        {
            return GetBlobContainerClient(blobContainerName).GetBlobBaseClient(blobName);
        }



        public virtual BlobContainerClient GetBlobContainerClient(string blobContainerName)
        {
            return blobServiceClient.GetBlobContainerClient(blobContainerName);
        }


        public virtual BlobClient GetBlobClient(string blobContainerName, string blobName)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            return blobContainerClient.GetBlobClient(blobName);
        }


        #endregion


    }
}
