using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Configuration;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace MultiMaster1
{
    class Program
    {
        public static Uri dburi = UriFactory.CreateDatabaseUri("ADW");
        public static Uri mystoreColUri = UriFactory.CreateDocumentCollectionUri("ADW", "conflict");


        private static void Main(string[] args)
        {
            var endpoint = ConfigurationManager.AppSettings["CosmosDbEndpoint"];
            var masterKey = ConfigurationManager.AppSettings["CosmosDbMasterKey"];


            // Set the read region selection preference order
            ConnectionPolicy connectionPolicy = new ConnectionPolicy();
            connectionPolicy.PreferredLocations.Add(LocationNames.CentralIndia); // first preference
            connectionPolicy.PreferredLocations.Add(LocationNames.SouthCentralUS); // second preference

            var client = new DocumentClient(new Uri(endpoint), masterKey, connectionPolicy);

            try
            {
                while (true)
                {
                    Console.WriteLine("Inserting data to Region");

                    CreateCollection(client).Wait();

                    CreateDocument(client).Wait();

                    Console.WriteLine("Data insertion completed press any key to exit");
                    var input = Console.ReadLine();
                    break;

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


        private static async Task CreateCollection(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Creating New Collection for Multi Master Test <<< ");
            //var opt = new RequestOptions {ConsistencyLevel= ConsistencyLevel.Eventual };
            //var opt = new RequestOptions {IndexingDirective = IndexingDirective.Exclude };

            DocumentCollection collectionDefinition = new DocumentCollection();
            collectionDefinition.Id = "conflict";
            collectionDefinition.IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) { Precision = -1 });
            collectionDefinition.PartitionKey.Paths.Add("/deviceId");
            collectionDefinition.ConflictResolutionPolicy = new ConflictResolutionPolicy
            {
                Mode = ConflictResolutionMode.Custom,
                ConflictResolutionProcedure = string.Format("dbs/{0}/colls/{1}/sprocs/{2}", "ADW", "conflict", "resolver") };

            await client.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri("ADW"), collectionDefinition, new RequestOptions { OfferThroughput = 400 });
        }

        private static async Task CreateDocument(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Creating New Collection for Multi Master Test <<< ");

            int i;
            i = 0;
            await client.CreateDocumentAsync(mystoreColUri,
            new DeviceReading { Id = i.ToString(), DeviceId = string.Format("xsensr-{0}", i), MetricType = "Temperature", Unit = "Celsius", MetricValue = 990, Region = "US" });

            

            i=1;
            await client.CreateDocumentAsync(mystoreColUri,
            new DeviceReading { Id = i.ToString(), DeviceId = string.Format("xsensr-{0}", i), MetricType = "Temperature", Unit = "Celsius", MetricValue = 990, Region = "India" });

            i=2;
            await client.CreateDocumentAsync(mystoreColUri,
            new DeviceReading { Id = i.ToString(), DeviceId = string.Format("xsensr-{0}", i), MetricType = "Temperature", Unit = "Celsius", MetricValue = 990, Region = "India" });

            i=3;
            await client.CreateDocumentAsync(mystoreColUri,
            new DeviceReading { Id = i.ToString(), DeviceId = string.Format("xsensr-{0}", i), MetricType = "Temperature", Unit = "Celsius", MetricValue = 990, Region = "India" });

            i=4;
            await client.CreateDocumentAsync(mystoreColUri,
            new DeviceReading { Id = i.ToString(), DeviceId = string.Format("xsensr-{0}", i), MetricType = "Temperature", Unit = "Celsius", MetricValue = 990, Region = "India" });
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

            [JsonProperty("region")]
            public string Region { get; set; }
        }
    }
}
