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

        CleanStart(storageConnectionString);

        UploadFileToStorageCurrent(storageConnectionString);
        UploadFileToStorageProposed(storageConnectionString);

        UploadMultipleCurrent(storageConnectionString);
        UploadMultipleProposed(storageConnectionString);

        UploadFileToStorageWithContentTypeCurrent(storageConnectionString);
        UploadFileToStorageWithContentTypeProposed(storageConnectionString);

        UploadFileToStorageBestSolution(storageConnectionString);
    }



    public static void UploadFileToStorageCurrent(string storageConnectionString)
    {
        string containerName = "logos";
        string blobName = "azure-logo.svg";
        string filePath = "images/azure.svg";

        BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnectionString);

        BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(containerName);
        blobContainerClient.CreateIfNotExists();  

        BlobClient blobClient = blobContainerClient.GetBlobClient(blobName);
        var response = blobClient.Upload(filePath);
    }



    public static void UploadFileToStorageProposed(string storageConnectionString)
    {
        string containerName = "logos";
        string blobName = "microsoft-logo.svg";
        string filePath = "images/microsoft-logo.svg";

        BlobStorageClient blobStorageClient = new BlobStorageClient(storageConnectionString);
        blobStorageClient.CreateBlobContainerIfNotExists(containerName);
        blobStorageClient.Upload(containerName, blobName, filePath);
    }


    public static void UploadMultipleCurrent(string storageConnectionString)
    {
        string containerName = "dev-languages";
        var csharp = (BlobName: "csharp.svg", FilePath: "images/csharp.svg");
        var golang = (BlobName: "golang.svg", FilePath: "images/golang.svg");
        var java = (BlobName: "java.svg", FilePath: "images/java.svg");
        var javascript = (BlobName: "javascript.svg", FilePath: "images/javascript.svg");
        var python = (BlobName: "python.svg", FilePath: "images/python.svg");
        var files = new[] { csharp, golang, java, javascript, python };

        // Here starts the code
        BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnectionString);

        BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(containerName);
        blobContainerClient.CreateIfNotExists();

        foreach (var file in files)
        {
            BlobClient blobClient = blobContainerClient.GetBlobClient(file.BlobName);
            var response = blobClient.Upload(file.FilePath);
        }        
    }

    public static void UploadMultipleProposed(string storageConnectionString)
    {
        string containerName = "dev-languages-new";
        var csharp = (BlobName: "csharp.svg", FilePath: "images/csharp.svg");
        var golang = (BlobName: "golang.svg", FilePath: "images/golang.svg");
        var java = (BlobName: "java.svg", FilePath: "images/java.svg");
        var javascript = (BlobName: "javascript.svg", FilePath: "images/javascript.svg");
        var python = (BlobName: "python.svg", FilePath: "images/python.svg");
        var files = new[] { csharp, golang, java, javascript, python };

        // Here starts the code
        BlobStorageClient blobStorageClient = new BlobStorageClient(storageConnectionString);

        blobStorageClient.CreateBlobContainerIfNotExists(containerName);

        foreach (var file in files)
            blobStorageClient.Upload(containerName, file.BlobName, file.FilePath);
        
        // Or even better
        // files.Select(f => blobStorageClient.Upload(containerName, f.BlobName, f.FilePath));

    }





    public static void UploadFileToStorageWithContentTypeCurrent(string storageConnectionString)
    {
        string containerName = "logos";
        string blobName = "azure-logo-with-content-type.svg";
        string filePath = "images/azure.svg";

        BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnectionString);

        BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(containerName);
        blobContainerClient.CreateIfNotExists();

        BlobClient blobClient = blobContainerClient.GetBlobClient(blobName);

        var options = new BlobUploadOptions() { HttpHeaders = new BlobHttpHeaders() { ContentType = "image/svg+xml" } };        
        var response = blobClient.Upload(filePath, options);
    }


    public static void UploadFileToStorageWithContentTypeProposed(string storageConnectionString)
    {
        string containerName = "logos";
        string blobName = "microsoft-logo-with-content-type.svg";
        string filePath = "images/microsoft-logo.svg";

        BlobStorageClient blobStorageClient = new BlobStorageClient(storageConnectionString);
        blobStorageClient.CreateBlobContainerIfNotExists(containerName);

        blobStorageClient.Upload(containerName, blobName, filePath, "image/svg+xml");
    }


    public static void UploadFileToStorageBestSolution(string storageConnectionString)
    {
        string containerName = "dev-languages";
        string blobName = "dotnet-core.svg";
        string filePath = "images/dotnet-core.svg";

        BlobStorageClient blobStorageClient = new BlobStorageClient(storageConnectionString, autoCreateContainers: true);
        blobStorageClient.Upload(containerName, blobName, filePath, "image/svg+xml");
    }



    public static void CleanStart(string storageConnectionString)
    {
        string logosContainerName = "logos";
        string[] logoBlobNames = new string[] { "azure-logo.svg", "microsoft-logo.svg", "microsoft-logo-with-content-type.svg", "dotnet-core.svg"};

        BlobStorageClient blobStorageClient = new BlobStorageClient(storageConnectionString);
        if ( blobStorageClient.ContainerExists(logosContainerName))
        {
            foreach (var blobName in logoBlobNames)
            {
                blobStorageClient.DeleteBlobIfExists(logosContainerName, blobName);
            }
            //blobStorageClient.DeleteBlobContainer(logosContainerName);
        }

        string languagesContainerName = "dev-languages";
        var csharp = (BlobName: "csharp.svg", FilePath: "images/csharp.svg");
        var golang = (BlobName: "golang.svg", FilePath: "images/golang.svg");
        var java = (BlobName: "java.svg", FilePath: "images/java.svg");
        var javascript = (BlobName: "javascript.svg", FilePath: "images/javascript.svg");
        var python = (BlobName: "python.svg", FilePath: "images/python.svg");
        var dotnet = (BlobName: "dotnet-core.svg", FilePath: "images/dotnet-core.svg");
        var languageBlobs = new[] { csharp, golang, java, javascript, python, dotnet };

        if (blobStorageClient.ContainerExists(languagesContainerName))
        {
            foreach (var lang in languageBlobs)
            {
                blobStorageClient.DeleteBlobIfExists(languagesContainerName, lang.BlobName);
            }
            //blobStorageClient.DeleteBlobContainer(logosContainerName);
        }


        string loopContainerName = "dev-languages-new";
        if (blobStorageClient.ContainerExists(loopContainerName))
        {
            foreach (var lang in languageBlobs)
            {
                blobStorageClient.DeleteBlobIfExists(loopContainerName, lang.BlobName);
            }
            //blobStorageClient.DeleteBlobContainer(logosContainerName);
        }
    }

}

