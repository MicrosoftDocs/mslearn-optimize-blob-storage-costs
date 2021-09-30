using System;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;


namespace ManageStorageTiers
{
    class Program
    {
        static void Main(string[] args)
        {
            DoWorkAsync().GetAwaiter().GetResult();

            Console.WriteLine("Press any key to exit the sample application.");
            Console.ReadLine();
        }

        static async Task DoWorkAsync()
        {
            // The BlobServiceClient object allows you to interact with the storage account
            string connectionString = Environment.GetEnvironmentVariable("STORAGE_CONNECTION_STRING");
            BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);

            // The BlobContainerClient allows us to interact with aspecific container in the storage account
            string blobContainerName = Environment.GetEnvironmentVariable("CONTAINER_NAME");
            BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);

            // Display the details of the blobs, including the storage tier
            Console.WriteLine("Initial blob storage tiers");
            await DisplayBlobTiers(blobContainerClient);

            Console.WriteLine("Press any key to proceed to updating blob storage tiers");
            Console.ReadKey();
            await UpdateBlobTiers(blobContainerClient);

            Console.WriteLine("Press any key to proceed to display the new blob storage tiers");
            Console.ReadKey();
            await DisplayBlobTiers(blobContainerClient);
        }

        private static async Task DisplayBlobTiers(BlobContainerClient blobContainerClient)
        {
            AsyncPageable<BlobItem> blobItems = blobContainerClient.GetBlobsAsync();

            await foreach (var blobItem in blobItems)
            {
                Console.WriteLine($"  Blob name {blobItem.Name}:   Tier {blobItem.Properties.AccessTier}");
            }
        }

        private static async Task UpdateBlobTiers(BlobContainerClient blobContainerClient)
        {
            AsyncPageable<BlobItem> blobItems = blobContainerClient.GetBlobsAsync();

            await foreach (var blobItem in blobItems)
            {
                string blobName = blobItem.Name;
                AccessTier? currentAccessTier = blobItem.Properties.AccessTier;
                AccessTier newAccessTier = GetNewAccessTier(currentAccessTier);

                Console.WriteLine($"  Blob name: {blobItem.Name}   Current tier: {currentAccessTier}   New tier: {newAccessTier}");

                BlobClient blobClient = blobContainerClient.GetBlobClient(blobItem.Name);
                blobClient.SetAccessTier(newAccessTier);
            }
        }


        private static AccessTier GetNewAccessTier(AccessTier? accessTier)
        {
            if (accessTier == AccessTier.Hot)
                return AccessTier.Cool;
            else if (accessTier == AccessTier.Cool)
                return AccessTier.Archive;
            else
                return AccessTier.Hot;
        }

    }
}
