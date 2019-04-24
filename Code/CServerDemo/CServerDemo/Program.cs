using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Configuration;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System.Diagnostics;
using System.IO;


namespace CServerDemo
{
    public static class Program
    {
        public static string DBName = "ADW";
        public static string CollectionName = "mynewstore";
        public static Uri mycoluri = UriFactory.CreateDocumentCollectionUri(DBName, CollectionName);
        

        static void Main(string[] args)
        {
            ShowMenu();
            var endpoint = ConfigurationManager.AppSettings["CosmosDBEndpoint"];
            var mkey = ConfigurationManager.AppSettings["CosmosDBMasterKey"];
            var client = new DocumentClient(new Uri(endpoint), mkey);
            

            while (true)
            { 
                try
                {
                    Console.WriteLine("Choose an option form the demo: ");
                    var input = Console.ReadLine();
                    var demoid = input.ToUpper().Trim();
                    

                    if (demoid == "CSP")
                    {
                        CreateHeloworldSP(client).Wait();
                    }
                    else if (demoid == "HSP")
                    {
                        Execute_spHelloWorld(client).Wait();
                    }
                    else if (demoid == "DVS")
                    {
                        Execute_spSetNorthAmerica(client).Wait();
                    }
                    else if (demoid == "BIS")
                    {
                        Execute_spBulkinsert(client).Wait();
                    }
                    else if (demoid == "DIS")
                    {
                        Execute_spBulkDelete(client).Wait();
                    }
                    else if (demoid == "CVT")
                    {
                        CreateTriggers(client).Wait();
                        ViewTriggers(client);
                    }
                    else if (demoid == "PRT")
                    {
                        Execute_trgValidateDocument(client).Wait();
                    }
                    else if (demoid == "POT")
                    {
                        Execute_trgUpdateMetadata(client).Wait();
                        DeleteTriggers(client).Wait();
                    }
                    else if (demoid == "ERF")
                    {
                        Execute_udfRegEx(client);
                    }
                    else if (demoid == "ENF")
                    {
                        Execute_udfIsNorthAmerica(client);
                    }
                    else if (demoid == "Q")
                    {
                        break;
                    }
                    else
                    {
                        Console.WriteLine("Please choose one of the available option");
                    }

                }
                catch (DocumentClientException de)
                {
                    Exception baseExp = de.GetBaseException();
                    Console.WriteLine($"{de.StatusCode} error occured: {de.Message} message: {baseExp.Message}");
                }

            }
        }

        private static void ShowMenu()
        {
            Console.WriteLine();
            Console.WriteLine(@" Choose one of the optoin to process the Demo request
CSP     To create SP from file.
HSP     To execute hello world SP
DVS     To execute document validiation and manupulation SP
BIS     To execute bulk insert SP
DIS     To delete bulk data using SP
CVT     To create and view trigger details from server side.
PRT     To execute pre trigger
POT     To Execute post trigger
ERF     To execyte UDF
ENF     Execute function to check is region is NA or not
Q       To Quit");
        }


        #region Stored Procdues
        private async static Task<StoredProcedure> CreateHeloworldSP(DocumentClient client)
        {
            var spbody = File.ReadAllText($@"..\..\ServerSideScripts\spHelloWorld.js");
            var spdef = new StoredProcedure
            {
                Id = "spHelloWorld",
                Body = spbody
            };

            var result = await client.CreateStoredProcedureAsync(mycoluri, spdef);


            Console.WriteLine($"Created stored proc {result.Resource.Id}; RID: {result.Resource.ResourceId}");

            return result;

        }

        private async static Task Execute_spHelloWorld(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine("Execute spHelloWorld stored procedure");

            var uri = UriFactory.CreateStoredProcedureUri(DBName, CollectionName, "spHelloWorld");
            var options = new RequestOptions { PartitionKey = new PartitionKey(string.Empty) };
            var result = await client.ExecuteStoredProcedureAsync<string>(uri, options);
            var message = result.Response;

            Console.WriteLine($"Result: {message}");
        }

        private async static Task Execute_spSetNorthAmerica(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine("Execute spSetNorthAmerica (country = United States)");

            // Should succeed with isNorthAmerica = true
            dynamic documentDefinition = new
            {
                name = "John Doe",
                address = new
                {
                    countryRegionName = "United States",
                    postalCode = "12345"
                }
            };

            var uri = UriFactory.CreateStoredProcedureUri(DBName, CollectionName, "spSetNorthAmerica");
            var opt = new RequestOptions { PartitionKey = new PartitionKey("12345") };
            var result = await client.ExecuteStoredProcedureAsync<object>(uri, opt, documentDefinition, true);
            var doc = result.Response;

            Console.Write("Results are");
            Console.Write($"Id = {doc.id}");
            Console.Write($"Country = {doc.address.countryRegionName}");
            Console.Write($"Is NA = {doc.address.isNorthAmerica}");

            string documentLink = doc._self;
            await client.DeleteDocumentAsync(documentLink, opt);
        }

        private async static Task Execute_spBulkinsert(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine("Execute spBulkInsert");

            var docs = new List<dynamic>();
            var total = 5000;
            for (var i = 1; i <= total; i++)
            {
                dynamic doc = new
                {
                    name = $"Bulk inserted doc {i}",
                    address = new
                    {
                        postalCode = "12345"
                    }
                };
                docs.Add(doc);
            }

            var uri = UriFactory.CreateStoredProcedureUri(DBName, CollectionName, "spBulkInsert");
            var options = new RequestOptions { PartitionKey = new PartitionKey("12345") };

            var totalInserted = 0;
            while (totalInserted < total)
            {
                var result = await client.ExecuteStoredProcedureAsync<int>(uri, options, docs);
                var inserted = result.Response;
                totalInserted += inserted;
                var remaining = total - totalInserted;
                Console.WriteLine($"Inserted {inserted} documents ({totalInserted} total, {remaining} remaining)");
                docs = docs.GetRange(inserted, docs.Count - inserted);
            }
        }

        private async static Task Execute_spBulkDelete(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine("Execute spBulkDelete");

            var uri = UriFactory.CreateStoredProcedureUri(DBName, CollectionName, "spBulkDelete");
            var options = new RequestOptions { PartitionKey = new PartitionKey("12345") };
            var sql = "SELECT VALUE c._self FROM c WHERE STARTSWITH(c.name, 'Bulk inserted doc ') = true";

            var continuationFlag = true;
            var totalDeleted = 0;
            while (continuationFlag)
            {
                var result = await client.ExecuteStoredProcedureAsync<spBulkDeleteResponse>(uri, options, sql);
                var response = result.Response;

                continuationFlag = response.ContinuationFlag;
                var deleted = response.Count;
                totalDeleted += deleted;
                Console.WriteLine($"Deleted {deleted} documents ({totalDeleted} total, more: {continuationFlag})");
            }

            Console.WriteLine($"Deleted bulk inserted documents; count: {totalDeleted}");
            Console.WriteLine();
        }
        #endregion

        #region Triggers
        private async static Task CreateTriggers(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Create Triggers <<<");
            Console.WriteLine();

            // Create pre-trigger
            var trgValidateDocument = File.ReadAllText(@"..\..\ServerSideScripts\trgValidateDocument.js");
            await CreateTrigger(client, "trgValidateDocument", trgValidateDocument, TriggerType.Pre, TriggerOperation.All);

            // Create post-trigger
            var trgUpdateMetadata = File.ReadAllText(@"..\..\ServerSideScripts\trgUpdateMetadata.js");
            await CreateTrigger(client, "trgUpdateMetadata", trgUpdateMetadata, TriggerType.Post, TriggerOperation.Create);
        }

        private async static Task<Trigger> CreateTrigger(DocumentClient client,string triggerId,string triggerBody,
            TriggerType triggerType, TriggerOperation triggerOperation)
        {
            var triggerDefinition = new Trigger
            {
                Id = triggerId,
                Body = triggerBody,
                TriggerType = triggerType,
                TriggerOperation = triggerOperation
            };

            var result = await client.CreateTriggerAsync(mycoluri, triggerDefinition);
            var trigger = result.Resource;
            Console.WriteLine($" Created trigger {trigger.Id}; RID: {trigger.ResourceId}");

            return trigger;
        }

        private static void ViewTriggers(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> View Triggers <<<");
            Console.WriteLine();

            var triggers = client.CreateTriggerQuery(mycoluri).ToList();

            foreach (var trigger in triggers)
            {
                Console.WriteLine($" Trigger: {trigger.Id};");
                Console.WriteLine($" RID: {trigger.ResourceId};");
                Console.WriteLine($" Type: {trigger.TriggerType};");
                Console.WriteLine($" Operation: {trigger.TriggerOperation}");
                Console.WriteLine();
            }
        }

        private static async Task Execute_trgValidateDocument(DocumentClient client)
        {
            // Create three documents
            var doc1Link = await CreateDocumentWithValidation(client, "mon");       // Monday
            var doc2Link = await CreateDocumentWithValidation(client, "THURS");     // Thursday
            var doc3Link = await CreateDocumentWithValidation(client, "sonday");    // error - won't get created

            // Update one of them
            await UpdateDocumentWithValidation(client, doc2Link, "FRI");            // Thursday > Friday

            // Delete them
            var requestOptions = new RequestOptions { PartitionKey = new PartitionKey("12345") };
            await client.DeleteDocumentAsync(doc1Link, requestOptions);
            await client.DeleteDocumentAsync(doc2Link, requestOptions);
        }

        private async static Task<string> CreateDocumentWithValidation(DocumentClient client, string weekdayOff)
        {
            dynamic documentDefinition = new
            {
                name = "John Doe",
                address = new { postalCode = "12345" },
                weekdayOff = weekdayOff
            };

            var options = new RequestOptions { PreTriggerInclude = new[] { "trgValidateDocument" } };

            try
            {
                var result = await client.CreateDocumentAsync(mycoluri, documentDefinition, options);
                var document = result.Resource;

                Console.WriteLine(" Result:");
                Console.WriteLine($"  Id = {document.id}");
                Console.WriteLine($"  Weekday off = {document.weekdayOff}");
                Console.WriteLine($"  Weekday # off = {document.weekdayNumberOff}");
                Console.WriteLine();

                return document._self;
            }
            catch (DocumentClientException ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                Console.WriteLine();

                return null;
            }
        }

        private async static Task UpdateDocumentWithValidation(DocumentClient client, string documentLink, string weekdayOff)
        {
            var sql = $"SELECT * FROM c WHERE c._self = '{documentLink}'";
            var feedOptions = new FeedOptions { EnableCrossPartitionQuery = true };
            var document = client.CreateDocumentQuery(mycoluri, sql, feedOptions).AsEnumerable().FirstOrDefault();

            document.weekdayOff = weekdayOff;

            var options = new RequestOptions { PreTriggerInclude = new[] { "trgValidateDocument" } };
            var result = await client.ReplaceDocumentAsync(document._self, document, options);
            document = result.Resource;

            Console.WriteLine(" Result:");
            Console.WriteLine($"  Id = {document.id}");
            Console.WriteLine($"  Weekday off = {document.weekdayOff}");
            Console.WriteLine($"  Weekday # off = {document.weekdayNumberOff}");
            Console.WriteLine();
        }

        private async static Task Execute_trgUpdateMetadata(DocumentClient client)
        {
            // Show no metadata documents
            ViewMetaDocs(client);

            // Create a bunch of documents across two partition keys
            var docs = new List<dynamic>
            {
				// 11229
				new { id = "11229a", address = new { postalCode = "11229" }, name = "New Customer ABCD" },
                new { id = "11229b", address = new { postalCode = "11229" }, name = "New Customer ABC" },
                new { id = "11229c", address = new { postalCode = "11229" }, name = "New Customer AB" },			// smallest
				new { id = "11229d", address = new { postalCode = "11229" }, name = "New Customer ABCDEF" },
                new { id = "11229e", address = new { postalCode = "11229" }, name = "New Customer ABCDEFG" },		// largest
				new { id = "11229f", address = new { postalCode = "11229" }, name = "New Customer ABCDE" },
				// 11235
				new { id = "11235a", address = new { postalCode = "11235" }, name = "New Customer AB" },
                new { id = "11235b", address = new { postalCode = "11235" }, name = "New Customer ABCDEFGHIJKL" },	// largest
				new { id = "11235c", address = new { postalCode = "11235" }, name = "New Customer ABC" },
                new { id = "11235d", address = new { postalCode = "11235" }, name = "New Customer A" },				// smallest
				new { id = "11235e", address = new { postalCode = "11235" }, name = "New Customer ABC" },
                new { id = "11235f", address = new { postalCode = "11235" }, name = "New Customer ABCDE" },
            };

            var options = new RequestOptions { PostTriggerInclude = new[] { "trgUpdateMetadata" } };
            foreach (var doc in docs)
            {
                await client.CreateDocumentAsync(mycoluri, doc, options);
            }

            // Show two metadata documents
            ViewMetaDocs(client);

            // Cleanup
            var sql = @"
				SELECT c._self, c.address.postalCode
				FROM c
				WHERE c.address.postalCode IN('11229', '11235')
			";

            var feedOptions = new FeedOptions { EnableCrossPartitionQuery = true };
            var documentKeys = client.CreateDocumentQuery(mycoluri, sql, feedOptions).ToList();
            foreach (var documentKey in documentKeys)
            {
                var requestOptions = new RequestOptions { PartitionKey = new PartitionKey(documentKey.postalCode) };
                await client.DeleteDocumentAsync(documentKey._self, requestOptions);
            }
        }

        private static void ViewMetaDocs(DocumentClient client)
        {
            var sql = @"SELECT * FROM c WHERE c.isMetaDoc";

            var feedOptions = new FeedOptions { EnableCrossPartitionQuery = true };
            var metaDocs = client.CreateDocumentQuery(mycoluri, sql, feedOptions).ToList();

            Console.WriteLine();
            Console.WriteLine($" Found {metaDocs.Count} metadata documents:");
            foreach (var metaDoc in metaDocs)
            {
                Console.WriteLine();
                Console.WriteLine($"  MetaDoc ID: {metaDoc.id}");
                Console.WriteLine($"  Metadata for: {metaDoc.address.postalCode}");
                Console.WriteLine($"  Smallest doc size: {metaDoc.minSize} ({metaDoc.minSizeId})");
                Console.WriteLine($"  Largest doc size: {metaDoc.maxSize} ({metaDoc.maxSizeId})");
                Console.WriteLine($"  Total doc size: {metaDoc.totalSize}");
            }
        }

        private async static Task DeleteTriggers(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Delete Triggers <<<");
            Console.WriteLine();

            await DeleteTrigger(client, "trgValidateDocument");
            await DeleteTrigger(client, "trgUpdateMetadata");
        }

        private async static Task DeleteTrigger(DocumentClient client, string triggerId)
        {
            var triggerUri = UriFactory.CreateTriggerUri(DBName, CollectionName, triggerId);

            await client.DeleteTriggerAsync(triggerUri);

            Console.WriteLine($"Deleted trigger: {triggerId}");
        }

        #endregion

        #region Functions
        private static void Execute_udfRegEx(DocumentClient client)
        {
            var sql = "SELECT c.id, c.name FROM c WHERE udf.udfRegEx(c.name, 'Rental') != null";

            Console.WriteLine();
            Console.WriteLine("Querying for Rental customers");
            var options = new FeedOptions { EnableCrossPartitionQuery = true };
            var documents = client.CreateDocumentQuery(mycoluri, sql, options).ToList();

            Console.WriteLine($"Found {documents.Count} Rental customers:");
            foreach (var document in documents)
            {
                Console.WriteLine($" {document.name} ({document.id})");
            }
        }

        private static void Execute_udfIsNorthAmerica(DocumentClient client)
        {
            var sql = @"
				SELECT c.name, c.address.countryRegionName
				FROM c
				WHERE udf.udfIsNorthAmerica(c.address.countryRegionName) = true";

            Console.WriteLine();
            Console.WriteLine("Querying for North American customers");
            var options = new FeedOptions { EnableCrossPartitionQuery = true };
            var documents = client.CreateDocumentQuery(mycoluri, sql, options).ToList();

            Console.WriteLine($"Found {documents.Count} North American customers; first 20:");
            foreach (var document in documents.Take(20))
            {
                Console.WriteLine($" {document.name}, {document.countryRegionName}");
            }

            sql = @"
				SELECT c.name, c.address.countryRegionName
				FROM c
				WHERE udf.udfIsNorthAmerica(c.address.countryRegionName) = false";

            Console.WriteLine();
            Console.WriteLine("Querying for non North American customers");
            documents = client.CreateDocumentQuery(mycoluri, sql, options).ToList();

            Console.WriteLine($"Found {documents.Count} non North American customers; first 20:");
            foreach (var document in documents.Take(20))
            {
                Console.WriteLine($" {document.name}, {document.countryRegionName}");
            }
        }

        #endregion
    }
    public class spBulkDeleteResponse
    {
        [JsonProperty(PropertyName = "count")]
        public int Count { get; set; }

        [JsonProperty(PropertyName = "continuationFlag")]
        public bool ContinuationFlag { get; set; }
    }

}
