using System;
using System.Linq;
using System.Threading.Tasks;

using System.Configuration;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System.Diagnostics;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Globalization;

namespace CIndexingDemo
{
    public static class Program
    {
        public static Uri dburi = UriFactory.CreateDatabaseUri("ADW");
        

        private static void Main(string[] args)
        {
            var endpoint = ConfigurationManager.AppSettings["CosmosDbEndpoint"];
            var masterKey = ConfigurationManager.AppSettings["CosmosDbMasterKey"];
            var client = new DocumentClient(new Uri(endpoint), masterKey);

            try
            {
                ShowMenu();
                while (true)
                {
                    Console.WriteLine("Choose an option form the demo: ");
                    var input = Console.ReadLine();
                    var demoid = input.ToUpper().Trim();
                    if (demoid == "EDI")
                    {
                        ExcludeDocFromIndex(client).Wait();
                    }
                    else if (demoid == "LIC")
                    {
                        UseLazyIndexing(client).Wait();
                    }
                    else if (demoid == "IPC")
                    {
                        ExcludePathsFromIndex(client).Wait();
                    }
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
            Console.WriteLine(@"Cosmos DB Indexing Demos

EDI Exclude Document From Index
LIC Create collection with Lazy indxing
IPC Craete collection with indexing path.
Q Quit
");
        }

        private static async Task ExcludeDocFromIndex(DocumentClient client)
        {
            
            string collectionId = string.Format(CultureInfo.InvariantCulture, "ExplicitlyExcludeFromIndex");
            var collectionUri = UriFactory.CreateDocumentCollectionUri("ADW", collectionId);

            Console.WriteLine("\n Exclude a document completely from the Index");

            // Create a collection with default index policy (i.e. automatic = true)
            DocumentCollection collection = await client.CreateDocumentCollectionAsync(dburi, new DocumentCollection { Id = collectionId });
            Console.WriteLine("Collection {0} created with index policy \n{1}", collection.Id, collection.IndexingPolicy);

            // Create a document Then query on it immediately
            // Will work as this Collection is set to automatically index everything
            Document created = await client.CreateDocumentAsync(collectionUri, new { id = "doc1", orderId = "order1" });
            Console.WriteLine("\nDocument created: \n{0}", created);

            bool found = client.CreateDocumentQuery(collectionUri, "SELECT * FROM root r WHERE r.orderId='order1'").AsEnumerable().Any();
            Console.WriteLine("Document found by query: {0}", found);

            // Now, create a document but this time explictly exclude it from the collection using IndexingDirective
            // Then query for that document
            // Shoud NOT find it, because we excluded it from the index
            // BUT, the document is there and doing a ReadDocument by Id will prove it
            created = await client.CreateDocumentAsync(collectionUri, new { id = "doc2", orderId = "order2" }, new RequestOptions
            {
                IndexingDirective = IndexingDirective.Exclude
            });
            Console.WriteLine("\nDocument created: \n{0}", created);

            found = client.CreateDocumentQuery(collectionUri, "SELECT * FROM root r WHERE r.orderId='order2'").AsEnumerable().Any();
            Console.WriteLine("Document found by query: {0}", found);

            Document document = await client.ReadDocumentAsync(created.SelfLink);
            Console.WriteLine("Document read by id: {0}", document != null);

            // Cleanup
            await client.DeleteDocumentCollectionAsync(collectionUri);
        }

        private static async Task UseLazyIndexing(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Creating collection named tcol in ADW database <<<");
            Console.WriteLine();
            Console.WriteLine("Throughtput: 1000 RU/Sec");
            Console.WriteLine("Partition Key: /Zipcodes");
            Console.WriteLine();

            var partitionKeyDef = new PartitionKeyDefinition();
            partitionKeyDef.Paths.Add("/zipcodes");

            var collectionDef = new DocumentCollection
            {
                Id = "tcol",
                PartitionKey = partitionKeyDef
            };
            //To change indexing policy from default Consistent to Lazy
            collectionDef.IndexingPolicy.IndexingMode = IndexingMode.Lazy;

            var ruDef = new RequestOptions { OfferThroughput = 1000 };

            var result = await client.CreateDocumentCollectionAsync(dburi, collectionDef, ruDef);
            var collection = result.Resource;

            Console.WriteLine("Created new collection");

            //it is very difficult to demonstrate lazy indexing as you only notice the difference under sustained heavy write load
            //because we're using an S1 collection in this demo we'd likely get throttled long before we were able to replicate sustained high throughput
            //which would give the index time to catch-up.

            await client.DeleteDocumentCollectionAsync(collection.SelfLink);
        }

        private static async Task ExcludePathsFromIndex(DocumentClient client)
        {
            string collectionId = string.Format(CultureInfo.InvariantCulture, "ExcludePathsFromIndex");
            var collectionUri = UriFactory.CreateDocumentCollectionUri("ADW", collectionId);

            Console.WriteLine("Exclude specified paths from document index");

            var collDefinition = new DocumentCollection { Id = collectionId };

            collDefinition.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/*" });  // Special manadatory path of "/*" required to denote include entire tree
            collDefinition.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/metaData/*" });   // exclude metaData node, and anything under it
            collDefinition.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/subDoc/nonSearchable/*" });  // exclude ONLY a part of subDoc    
            collDefinition.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/\"excludedNode\"/*" }); // exclude excludedNode node, and anything under it

            // The effect of the above IndexingPolicy is that only id, foo, and the subDoc/searchable are indexed

            var result = await client.CreateDocumentCollectionAsync(dburi, collDefinition);
            var collection = result.Resource;
            Console.WriteLine("Collection {0} created with index policy \n{1}", collection.Id, collection.IndexingPolicy);

            int numDocs = 250;
            Console.WriteLine("Creating {0} documents", numDocs);
            for (int docIndex = 0; docIndex < numDocs; docIndex++)
            {
                dynamic dyn = new
                {
                    id = "doc" + docIndex,
                    foo = "bar" + docIndex,
                    metaData = "meta" + docIndex,
                    subDoc = new { searchable = "searchable" + docIndex, nonSearchable = "value" + docIndex },
                    excludedNode = new { subExcluded = "something" + docIndex, subExcludedNode = new { someProperty = "value" + docIndex } }
                };
                Document created = await client.CreateDocumentAsync(collection.SelfLink, dyn);
                Console.WriteLine("Creating document with id {0}", created.Id);
            }

            // Querying for a document on either metaData or /subDoc/subSubDoc/someProperty will be expensive since they do not utilize the index,
            // but instead are served from scan automatically.
            int queryDocId = numDocs / 2;
            QueryStats queryStats = await GetQueryResult(collection, string.Format(CultureInfo.InvariantCulture, "SELECT * FROM root r WHERE r.metaData='meta{0}'", queryDocId), client);
            Console.WriteLine("Query on metaData returned {0} results", queryStats.Count);
            Console.WriteLine("Query on metaData consumed {0} RUs", queryStats.RequestCharge);

            queryStats = await GetQueryResult(collection, string.Format(CultureInfo.InvariantCulture, "SELECT * FROM root r WHERE r.subDoc.nonSearchable='value{0}'", queryDocId), client);
            Console.WriteLine("Query on /subDoc/nonSearchable returned {0} results", queryStats.Count);
            Console.WriteLine("Query on /subDoc/nonSearchable consumed {0} RUs", queryStats.RequestCharge);

            queryStats = await GetQueryResult(collection, string.Format(CultureInfo.InvariantCulture, "SELECT * FROM root r WHERE r.excludedNode.subExcludedNode.someProperty='value{0}'", queryDocId), client);
            Console.WriteLine("Query on /excludedNode/subExcludedNode/someProperty returned {0} results", queryStats.Count);
            Console.WriteLine("Query on /excludedNode/subExcludedNode/someProperty cost {0} RUs", queryStats.RequestCharge);

            // Querying for a document using food, or even subDoc/searchable > consume less RUs because they were not excluded
            queryStats = await GetQueryResult(collection, string.Format(CultureInfo.InvariantCulture, "SELECT * FROM root r WHERE r.foo='bar{0}'", queryDocId), client);
            Console.WriteLine("Query on /foo returned {0} results", queryStats.Count);
            Console.WriteLine("Query on /foo cost {0} RUs", queryStats.RequestCharge);

            queryStats = await GetQueryResult(collection, string.Format(CultureInfo.InvariantCulture, "SELECT * FROM root r WHERE r.subDoc.searchable='searchable{0}'", queryDocId), client);
            Console.WriteLine("Query on /subDoc/searchable returned {0} results", queryStats.Count);
            Console.WriteLine("Query on /subDoc/searchable cost {0} RUs", queryStats.RequestCharge);

            //Cleanup
            await client.DeleteDocumentCollectionAsync(collectionUri);
        }

        struct QueryStats
        {
            public QueryStats(int count, double requestCharge)
            {
                Count = count;
                RequestCharge = requestCharge;
            }

            public readonly int Count;
            public readonly double RequestCharge;
        };

        private static async Task<QueryStats> GetQueryResult(DocumentCollection collection, string query, DocumentClient client)
        {
            try
            {
                IDocumentQuery<dynamic> documentQuery = client.CreateDocumentQuery(
                    collection.SelfLink,
                    query,
                    new FeedOptions
                    {
                        PopulateQueryMetrics = true,
                        MaxItemCount = -1
                    }).AsDocumentQuery();

                FeedResponse<dynamic> response = await documentQuery.ExecuteNextAsync();
                return new QueryStats(response.Count, response.RequestCharge);
            }
            catch (Exception e)
            {
                LogException(e);
                return new QueryStats(0, 0.0);
            }
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

    }
}
