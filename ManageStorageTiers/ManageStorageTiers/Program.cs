using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

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
            CloudBlobClient cloudBlobClient = null;
            CloudBlobContainer cloudBlobContainer = null;

            // Connect to the user's storage account and create a reference to the blob container holding the sample blobs
            try
            {
                Console.WriteLine("Connecting to blob storage");
                string storageAccountConnectionString = Environment.GetEnvironmentVariable("CONNECTION_STRING");
                string blobContainerName = Environment.GetEnvironmentVariable("CONTAINER_NAME");

                CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(storageAccountConnectionString);
                cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
                cloudBlobContainer = cloudBlobClient.GetContainerReference(blobContainerName);
                Console.WriteLine("Connected");
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Failed to connect to storage account or container: {ex.Message}");
                return;
            }

            // Display the details of the blobs, including the storage tier
            await DisplayBlobTiers(cloudBlobContainer);

            try
            {
                // Change the storage tier for each blob:
                // Set blob1 to Cool (was Archive)
                // Note the the effects of this change are not immediate - it can take several hours for a blob to be rehydrated, during which time it remains in the Archive storage tier
                Console.WriteLine("Moving blob1 to the Cool tier");
                CloudBlockBlob blob1 = cloudBlobContainer.GetBlockBlobReference("blob1");
                blob1.SetStandardBlobTier(StandardBlobTier.Cool);

                // Set blob2 to Hot (was Cool)
                Console.WriteLine("Moving blob2 to the Hot tier");
                CloudBlockBlob blob2 = cloudBlobContainer.GetBlockBlobReference("blob2");
                blob2.SetStandardBlobTier(StandardBlobTier.Hot);

                // Set blob3 to Archive (was Hot)
                Console.WriteLine("Moving blob3 to the Archive tier");
                CloudBlockBlob blob3 = cloudBlobContainer.GetBlockBlobReference("blob3");
                blob3.SetStandardBlobTier(StandardBlobTier.Archive);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to change storage tier of blobs: {ex.Message}");
                return;
            }

            // Display the details of the blobs again, and confirm that the storage tier for the  Cool and Hot blobs has changed.
            // The Archive blob (blob1), might still be reported as being in the Archive tier if it has not yet been rehydrated 
            await DisplayBlobTiers(cloudBlobContainer);
        }

        private static async Task DisplayBlobTiers(CloudBlobContainer cloudBlobContainer)
        {
            try
            {
                // Find the details, including the tier level, for each blob
                Console.WriteLine("Fetching the details of all blobs");
                BlobContinuationToken blobContinuationToken = null;
                do
                {
                    var results = await cloudBlobContainer.ListBlobsSegmentedAsync(null, blobContinuationToken);
                    blobContinuationToken = results.ContinuationToken;
                    foreach (CloudBlockBlob item in results.Results)
                    {
                        // The StandardBlobTier property indicates the blob tier - Archive, Cool, or Hot
                        Console.WriteLine($"Blob name {item.Name}: Tier {item.Properties.StandardBlobTier}");
                    }
                } while (blobContinuationToken != null);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to retrieve details of blobs: {ex.Message}");
                return;
            }
        }
    }
}
