using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using System.Configuration;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace TTLandChangeFeed
{
    class Program
    {
        public static Uri dburi = UriFactory.CreateDatabaseUri("ADW");
        public static Uri mystoreColUri = UriFactory.CreateDocumentCollectionUri("ADW", "TTLandChangefeed");


        private static void Main(string[] args)
        {
            var endpoint = ConfigurationManager.AppSettings["CosmosDbEndpoint"];
            var masterKey = ConfigurationManager.AppSettings["CosmosDbMasterKey"];
            var client = new DocumentClient(new Uri(endpoint), masterKey);

            // Set the read region selection preference order
            //connectionPolicy.PreferredLocations.Add(LocationNames.EastUS); // first preference
            //connectionPolicy.PreferredLocations.Add(LocationNames.NorthEurope); // second preference
            //connectionPolicy.PreferredLocations.Add(LocationNames.SoutheastAsia); // third preference


            try
            {
                ShowMenu();
                while (true)
                {
                    Console.WriteLine("Choose an option form the demo: ");
                    var input = Console.ReadLine();
                    var demoid = input.ToUpper().Trim();
                    if (demoid == "CCT")
                    {
                        CreateCollections(client).Wait();
                    }
                    else if (demoid == "CDT")
                    {
                        CreateDocument(client).Wait();
                    }
                    else if (demoid == "CX")
                    {
                        DeleteCollection(client).Wait();
                    }
                    else if (demoid == "CFD")
                    {
                        ChangeFeedDemo("ADW", "TTLandChangefeed", client).Wait();
                    }
                    //else if (demoid == "LC")
                    //{
                    //    ViewCollections(client);
                    //}
                    else if (demoid == "Q")
                    {
                        break;
                    }
                    else
                    {
                        Console.WriteLine($"{input} is Invalide, please choose from the list above");
                    }
                }
            }
            catch (DocumentClientException de)

            {
                Exception baseException = de.GetBaseException();
                Console.WriteLine($"{de.StatusCode} error occured: {de.Message}, Message: {baseException.Message}");
            }
            finally
            {
                Console.WriteLine();
                Console.WriteLine("You have choosed to end the Demo: press any key to exit.");
                Console.ReadKey(true);
            }
        }


        private static void ShowMenu()
        {
            Console.WriteLine(@"Cosmos DB SQL API .NET Demos

CCT Create collection with TTL.
CDT CreateDocument by over writting the TTL.
CFD Changefeed demo.
CX  Clean up the environment.
Q Quit
");
        }
        #region Collecions demo
        private static void ViewCollections(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(" >> Listing all collections from ADW database <<<");

            var collections = client.CreateDocumentCollectionQuery(dburi).ToList();

            var i = 0;
            foreach (var collection in collections)
            {
                i++;

                Console.WriteLine();
                Console.WriteLine($" Collections #{i}");
                ViewCollection(collection);
            }
            Console.WriteLine();
            Console.WriteLine($"Total collections in mydb database: {collections.Count}");
        }

        private static void ViewCollection(DocumentCollection collection)
        {
            Console.WriteLine($"    Collection ID: {collection.Id}");
            Console.WriteLine($"      Resource ID: {collection.ResourceId}");
            Console.WriteLine($"        Self Link: {collection.SelfLink}");
            Console.WriteLine($"            E-Tag: {collection.ETag}");
            Console.WriteLine($"        Timestamp: {collection.Timestamp}");
            Console.WriteLine($" TimeToLive value: {collection.DefaultTimeToLive}");
        }


        private static async Task CreateCollections(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Creating collection named tcol in ADW database <<<");
            Console.WriteLine();
            Console.WriteLine("Throughtput: 2500 RU/Sec");
            Console.WriteLine("Partition Key: /deviceId");
            Console.WriteLine("Set TTL to 10,000 Sec");
            Console.WriteLine();

            DocumentCollection collectionDefinition = new DocumentCollection();
            collectionDefinition.Id = "TTLandChangefeed";
            collectionDefinition.PartitionKey.Paths.Add("/deviceId");

            //We are setting the TTL at container level to 10000 Seconds
            //If we set this value -1, you are document will never be deleted autometiaclly.
            collectionDefinition.DefaultTimeToLive = 10000;

            var result = await client.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri("ADW"),
                collectionDefinition,
                new RequestOptions { OfferThroughput = 2500 });

            var collection = result.Resource;

            Console.WriteLine("Created new collection");
            ViewCollection(collection);
        }

        private static async Task DeleteCollection(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Deleting collection named tcol in ADW database <<<");

            var collectionURI = UriFactory.CreateDocumentCollectionUri("ADW", "TTLandChangefeed");
            await client.DeleteDocumentCollectionAsync(collectionURI);

            Console.WriteLine(">>> Collection named tcol in ADW database is deleted sucessfully..<<<");
        }
        #endregion

        private static async Task CreateDocument(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Creating New ducument into mystore collection of ADW <<< ");

            Console.WriteLine("Inserting 100 documents");
            List<Task> insertTasks = new List<Task>();

            Uri collectionUri = UriFactory.CreateDocumentCollectionUri("ADW", "TTLandChangefeed");

            for (int i = 0; i < 100; i++)
            {
                if (i==1)
                {
                    insertTasks.Add(client.CreateDocumentAsync(collectionUri,
                        new DeviceReadingTTL
                        { DeviceId = string.Format("xsensr-{0}", i),
                            MetricType = "Temperature", Unit = "Celsius", MetricValue = 990,
                            TTL=20}));
                }
                else
                    {
                    insertTasks.Add(client.CreateDocumentAsync(collectionUri,
                        new DeviceReading { DeviceId = string.Format("xsensr-{0}", i), MetricType = "Temperature", Unit = "Celsius", MetricValue = 990 }));
                };
            }

            await Task.WhenAll(insertTasks);            
            Console.WriteLine();
        }

        private static async Task ChangeFeedDemo(string databaseId, string collectionId,DocumentClient client)
        {
            Uri collectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);

            // Returns all documents in the collection.
            Console.WriteLine("Reading all changes from the beginning");
            Dictionary<string, string> checkpoints = await GetChanges(client, collectionUri, new Dictionary<string, string>());

            Console.WriteLine("Inserting 2 new documents");
            await client.CreateDocumentAsync(
                collectionUri,
                new DeviceReading { DeviceId = "xsensr-201", MetricType = "Temperature", Unit = "Celsius", MetricValue = 1000 });
            await client.CreateDocumentAsync(
                collectionUri,
                new DeviceReading { DeviceId = "xsensr-212", MetricType = "Pressure", Unit = "psi", MetricValue = 1000 });

            // Returns only the two documents created above.
            Console.WriteLine("Reading changes using Change Feed from the last checkpoint");
            checkpoints = await GetChanges(client, collectionUri, checkpoints);
        }

        // Get changes within the collection since the last checkpoint. This sample shows how to process the change 
        // feed from a single worker. When working with large collections, this is typically split across multiple workers each processing a single or set of partition key ranges.
        private static async Task<Dictionary<string, string>> GetChanges(
            DocumentClient client,
            Uri collectionUri,
            Dictionary<string, string> checkpoints)
        {
            int numChangesRead = 0;
            string pkRangesResponseContinuation = null;
            List<PartitionKeyRange> partitionKeyRanges = new List<PartitionKeyRange>();

            do
            {
                FeedResponse<PartitionKeyRange> pkRangesResponse = await client.ReadPartitionKeyRangeFeedAsync(
                    collectionUri,
                    new FeedOptions { RequestContinuation = pkRangesResponseContinuation });

                partitionKeyRanges.AddRange(pkRangesResponse);
                pkRangesResponseContinuation = pkRangesResponse.ResponseContinuation;
            }
            while (pkRangesResponseContinuation != null);

            foreach (PartitionKeyRange pkRange in partitionKeyRanges)
            {
                string continuation = null;
                checkpoints.TryGetValue(pkRange.Id, out continuation);

                IDocumentQuery<Document> query = client.CreateDocumentChangeFeedQuery(
                    collectionUri,
                    new ChangeFeedOptions
                    {
                        PartitionKeyRangeId = pkRange.Id,
                        StartFromBeginning = true,
                        RequestContinuation = continuation,
                        MaxItemCount = -1,
                        // Set reading time: only show change feed results modified since StartTime
                        StartTime = DateTime.Now - TimeSpan.FromSeconds(30)
                    });

                while (query.HasMoreResults)
                {
                    FeedResponse<DeviceReading> readChangesResponse = query.ExecuteNextAsync<DeviceReading>().Result;

                    foreach (DeviceReading changedDocument in readChangesResponse)
                    {
                        Console.WriteLine("\tRead document {0} from the change feed.", changedDocument.Id);
                        numChangesRead++;
                    }

                    checkpoints[pkRange.Id] = readChangesResponse.ResponseContinuation;
                }
            }

            Console.WriteLine("Read {0} documents from the change feed", numChangesRead);

            return checkpoints;
        }

        private static void LogException(Exception e)
        {
            ConsoleColor color = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Red;

            Exception baseException = e.GetBaseException();
            if (e is DocumentClientException)
            {
                DocumentClientException de = (DocumentClientException)e;
                Console.WriteLine("{0} error occurred: {1}, Message: {2}", de.StatusCode, de.Message, baseException.Message);
            }
            else
            {
                Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }

            Console.ForegroundColor = color;
        }

        public class DeviceReading
        {
            [JsonProperty("id")]
            public string Id { get; set; }

            [JsonProperty("deviceId")]
            public string DeviceId { get; set; }

            [JsonConverter(typeof(IsoDateTimeConverter))]
            [JsonProperty("readingTime")]
            public DateTime ReadingTime { get; set; }

            [JsonProperty("metricType")]
            public string MetricType { get; set; }

            [JsonProperty("unit")]
            public string Unit { get; set; }

            [JsonProperty("metricValue")]
            public double MetricValue { get; set; }
        }

        public class DeviceReadingTTL
        {
            [JsonProperty("id")]
            public string Id { get; set; }

            [JsonProperty("deviceId")]
            public string DeviceId { get; set; }

            [JsonConverter(typeof(IsoDateTimeConverter))]
            [JsonProperty("readingTime")]
            public DateTime ReadingTime { get; set; }

            [JsonProperty("metricType")]
            public string MetricType { get; set; }

            [JsonProperty("unit")]
            public string Unit { get; set; }

            [JsonProperty("metricValue")]
            public double MetricValue { get; set; }

            [JsonProperty("ttl")]
            public int TTL { get; set; }
        }
    }
}

