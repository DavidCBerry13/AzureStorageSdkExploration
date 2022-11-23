using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using BlobStorageExtensions;
using Azure.Storage.Blobs.Models;
using System.Runtime.CompilerServices;
using static System.Net.WebRequestMethods;

namespace MyProject;
class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("Azure SDK Storage Demo");
        string? storageConnectionString = Environment.GetEnvironmentVariable("STORAGE_CONNECTION_STRING");

        if (string.IsNullOrWhiteSpace(storageConnectionString))
        {
            Console.WriteLine("Put your storage connection string in the environment variable STORAGE_CONNECTION_STRING");
            return;
        }

        CleanStart(storageConnectionString, logosContainer, devLanguagesContainer, devLanguagescontainer2);

        UploadFileToStorageCurrent(storageConnectionString, logosContainer, "azure-logo.svg", "images/azure.svg");
        UploadFileToStorageProposed(storageConnectionString, logosContainer, "microsoft-logo.svg", "images/microsoft-logo.svg");

        UploadMultipleCurrent(storageConnectionString, devLanguagesContainer, devLanguagesImages);
        UploadMultipleProposed(storageConnectionString, devLanguagescontainer2, devLanguagesImages);

        UploadFileToStorageWithContentTypeCurrent(storageConnectionString, logosContainer, "azure-logo-with-content-type.svg", "images/azure.svg");
        UploadFileToStorageWithContentTypeProposed(storageConnectionString, logosContainer, "microsoft-logo-with-content-type.svg", "images/microsoft-logo.svg");

        UploadFileToStorageBestSolution(storageConnectionString, devLanguagesContainer, "dotnet-core.svg", "images/dotnet-core.svg");

        Console.WriteLine("\nDev Language Blobs");
        Console.WriteLine("----------------------------------------------------------------------");
        GetBlobs(storageConnectionString, "dev-languages");
    }

    private static string logosContainer = "logos";
    private static string devLanguagesContainer = "dev-languages";
    private static string devLanguagescontainer2 = "dev-languages-new";
    private static string[] devLanguagesImages = new[] { "images/csharp.svg", "images/golang.svg", "images/java.svg", "images/javascript.svg", "images/python.svg" };





    public static void UploadFileToStorageCurrent(string storageConnectionString, string containerName, string blobName, string filePath)
    {
        BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnectionString);

        BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(containerName);
        blobContainerClient.CreateIfNotExists();  

        BlobClient blobClient = blobContainerClient.GetBlobClient(blobName);
        var response = blobClient.Upload(filePath);
    }



    public static void UploadFileToStorageProposed(string storageConnectionString, string containerName, string blobName, string filePath)
    {
        BlobStorageClient blobStorageClient = new BlobStorageClient(storageConnectionString);
        blobStorageClient.CreateBlobContainerIfNotExists(containerName);
        blobStorageClient.Upload(containerName, blobName, filePath);
    }


    public static void UploadMultipleCurrent(string storageConnectionString, string containerName, string[] files)
    {
        BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnectionString);

        BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(containerName);
        blobContainerClient.CreateIfNotExists();

        foreach (var file in files)
        {
            var blobName = Path.GetFileName(file);
            var filePath = file;

            BlobClient blobClient = blobContainerClient.GetBlobClient(blobName);
            var response = blobClient.Upload(file);
        }        
    }

    public static void UploadMultipleProposed(string storageConnectionString, string containerName, string[] files)
    {
        // Here starts the code
        BlobStorageClient blobStorageClient = new BlobStorageClient(storageConnectionString);

        blobStorageClient.CreateBlobContainerIfNotExists(containerName);

        foreach (var file in files)
        {
            var blobName = Path.GetFileName(file);
            var filePath = file;

            blobStorageClient.Upload(containerName, blobName, filePath);
        }

        //foreach (var file in files)
        //    blobStorageClient.Upload(containerName, Path.GetFileName(file), file);

        // Or even better
        // files.Select(file => blobStorageClient.Upload(containerName, Path.GetFileName(file), file));
    }





    public static void UploadFileToStorageWithContentTypeCurrent(string storageConnectionString, string containerName, string blobName, string filePath)
    {
        BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnectionString);

        BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(containerName);
        blobContainerClient.CreateIfNotExists();

        BlobClient blobClient = blobContainerClient.GetBlobClient(blobName);
        var options = new BlobUploadOptions() { HttpHeaders = new BlobHttpHeaders() { ContentType = "image/svg+xml" } };        
        var response = blobClient.Upload(filePath, options);
    }


    public static void UploadFileToStorageWithContentTypeProposed(string storageConnectionString, string containerName, string blobName, string filePath)
    {
        BlobStorageClient blobStorageClient = new BlobStorageClient(storageConnectionString);
        blobStorageClient.CreateBlobContainerIfNotExists(containerName);

        blobStorageClient.Upload(containerName, blobName, filePath, "image/svg+xml");
    }


    public static void UploadFileToStorageBestSolution(string storageConnectionString, string containerName, string blobName, string filePath)
    {
        BlobStorageClient blobStorageClient = new BlobStorageClient(storageConnectionString, autoCreateContainers: true);
        blobStorageClient.Upload(containerName, blobName, filePath, "image/svg+xml");
    }


    public static void GetBlobs(string storageConnectionString, string containerName)
    {
        BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnectionString);

        BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(containerName);
        var blobs = blobContainerClient.GetBlobs();

        foreach (var blob in blobs)
        {
            Console.WriteLine($"{blob.Name.PadRight(20)}    {blob.Properties.CreatedOn}   {blob.Properties.ContentLength}  {blob.Properties.AccessTier?.ToString()}");
        }
    }



    public static void CleanStart(string storageConnectionString, params string[] containers)
    {
        BlobStorageClient blobStorageClient = new BlobStorageClient(storageConnectionString);
        foreach (var container in containers)
        {
            if (blobStorageClient.ContainerExists(container))
            {
                var blobs = blobStorageClient.GetBlobs(container);

                foreach (var blob in blobs)
                {
                    blobStorageClient.DeleteBlobIfExists(container, blob.Name);
                }
                //blobStorageClient.DeleteBlobContainer(logosContainerName);
            }
        }
    }






}

