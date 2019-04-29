using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Configuration;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System.Diagnostics;
using Newtonsoft.Json;

namespace CDemo1
{
    public static class Program
    {
        public static Uri dburi = UriFactory.CreateDatabaseUri("ADW");
        public static Uri mystoreColUri = UriFactory.CreateDocumentCollectionUri("ADW", "stores");


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
                    if (demoid == "DBL")
                    {
                        ViewDatabases(client);
                    }
                    else if (demoid == "DBC")
                    {
                        CreateDatabse(client).Wait();
                    }
                    else if (demoid == "DBD")
                    {
                        DeleteDatbase(client).Wait();
                    }
                    else if (demoid == "LC")
                    {
                        ViewCollections(client);
                    }
                    else if (demoid == "CC")
                    {
                        CreateCollections(client).Wait();
                    }
                    else if (demoid == "DC")
                    {
                        DeleteCollection(client).Wait();
                    }
                    else if (demoid == "CDO")
                    {
                        CreateDocument(client).Wait();
                    }
                    else if (demoid == "QDO")
                    {
                        QueryDocumentsByProperty(client);
                    }
                    else if (demoid == "QDP")
                    {
                        QueryDocumentsWithPaging(client).Wait();
                    }
                    else if (demoid == "QDR")
                    {
                        QueryDocumentsReplace(client).Wait();
                    }
                    else if (demoid == "DUR")
                    {
                        UpsertDocument(client).Wait();
                    }
                    else if (demoid == "DDO")
                    {
                        DeleteDcouments(client).Wait();
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
            Console.WriteLine(@"Cosmos DB SQL API .NET Demos

DBL List all databases.
DBC Create database named myTestDB.
DBD Delete database Named myTestDB.
LC  List all collections in ADW database.
CC  Create collection tcol under ADW database.
DC  Delete collection tcol from adw database.
CDO Create documents in mystore collection of ADW.
QDO Query documents in mystore collection of ADW by property filter.
QDP Query documents with paging option.
QDR Query the documents in mystore collection of ADW by property filter and repalce.
DUR Query the documents in mystore collection of ADW by property filter and upsert.
DDO Query the documents in mystore collection of ADW by property filter and delete.
Q Quit
");
        }



        #region Database demo
        private static void ViewDatabases(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> View Databases <<<");

            var databases = client.CreateDatabaseQuery().ToList();
            foreach (var database in databases)
            {
                Console.WriteLine($" Database Id: {database.Id}; Rid: {database.ResourceId}");
            }

            Console.WriteLine();
            Console.WriteLine($"Total databases: {databases.Count}");
        }

        private async static Task CreateDatabse(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Creating datbase named myTestDB <<<");

            var databaseDefination = new Database { Id = "myTestDB" };
            var result = await client.CreateDatabaseAsync(databaseDefination);
            var database = result.Resource;

            Console.WriteLine($" Database name: {database.Id}; Resource ID of the DB is: {database.ResourceId}");
            Console.WriteLine();
        }

        private async static Task DeleteDatbase(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Deleting database myTestDB <<<");

            var dburi = UriFactory.CreateDatabaseUri("myTestDB");
            await client.DeleteDatabaseAsync(dburi);

            Console.WriteLine();
        }
        #endregion

        #region Collecions demo
        private static void ViewCollections(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(" >> Listing all collections from ADW database <<<");

            var collections = client.CreateDocumentCollectionQuery(dburi).ToList();

            var i = 0;
            foreach(var collection in collections)
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
        }


        private static async Task CreateCollections(DocumentClient client)
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
            //collectionDef.IndexingPolicy.IndexingMode = IndexingMode.Lazy;

            var ruDef = new RequestOptions { OfferThroughput = 1000 };

            var result = await client.CreateDocumentCollectionAsync(dburi, collectionDef, ruDef);
            var collection = result.Resource;

            Console.WriteLine("Created new collection");
            ViewCollection(collection);
        }

        private static async Task DeleteCollection(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Deleting collection named tcol in ADW database <<<");

            var collectionURI = UriFactory.CreateDocumentCollectionUri("ADW", "tcol");
            await client.DeleteDocumentCollectionAsync(collectionURI);

            Console.WriteLine(">>> Collection named tcol in ADW database is deleted sucessfully..<<<");
        }
        #endregion

        #region Documents demo
        private static async Task CreateDocument(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Creating New ducument into mystore collection of ADW <<< ");
            //var opt = new RequestOptions {ConsistencyLevel= ConsistencyLevel.Eventual };
            //var opt = new RequestOptions {IndexingDirective = IndexingDirective.Exclude };

            dynamic documentdef = new {
                name = "New Customer 1",
                address = new
                {
                    addressType = "Main Office",
                    addressLine1 = "123 Main Street",
                    location = new
                    {
                        city = "Bangalore",
                        stateProvinceName = "Bangalore"
                    },
                    postalCode = "560001",
                    countryRegionName = "India"
                },
            };

            Document dc1 = await client.CreateDocumentAsync(mystoreColUri, documentdef);
            Console.WriteLine($"Created document {dc1.Id} from dynamic object");
            Console.WriteLine();

            var doc2def = @"
            {
				""name"": ""New Customer 2"",
				""address"": {
					""addressType"": ""Main Office"",
					""addressLine1"": ""123 Main Street"",
					""location"": {
						""city"": ""Hydrabad"",
						""stateProvinceName"": ""Hydrabad""
					},
					""postalCode"": ""11229"",
					""countryRegionName"": ""India""
				}
			}";

            var doc2defobj = JsonConvert.DeserializeObject(doc2def);
            Document dc2 = await client.CreateDocumentAsync(mystoreColUri, doc2defobj);
           
            Console.WriteLine($"Created document {dc2.Id} from JSON String ");
            Console.WriteLine();
        }

        private static void QueryDocumentsByProperty(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Query Documents (SQL) <<<");
            Console.WriteLine();

            Console.WriteLine("Querting documents by property filter");
            var sql = "SELECT * FROM c WHERE c.address.countryRegionName = 'India' and c.address.location.stateProvinceName = 'Bangalore'";
            //var option = new FeedOptions { EnableCrossPartitionQuery = true};


            var documents = client.CreateDocumentQuery(mystoreColUri, sql).ToList();
            Console.WriteLine($"Found {documents.Count} number of documents");
            
            foreach (var doc in documents)
            {
                Console.WriteLine($" ID: {doc.id}; Name: {doc.name}");
                Console.WriteLine($" Contry: {doc.address.countryRegionName} ");
                Console.WriteLine();
            }



            //Console.WriteLine("Querting all document in the colelction");
            //sql = "SELECT * FROM c";
            
            //documents = client.CreateDocumentQuery(mystoreColUri, sql, option).ToList();
            //Console.WriteLine($"Found {documents.Count} number of documents");
            //foreach (var doc in documents)
            //{
            //    Console.WriteLine($" ID: {doc.id}; Name: {doc.name}");
            //}

        }

        private static async Task QueryDocumentsReplace(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Query Documents and Replace<<<");
            Console.WriteLine();

            Console.WriteLine("Querting documents by property filter");
            var sql = "SELECT * FROM c WHERE c.address.countryRegionName = 'India' and c.address.location.stateProvinceName = 'Bangalore'";
            //var option = new FeedOptions { EnableCrossPartitionQuery = true};


            var documents = client.CreateDocumentQuery(mystoreColUri, sql).AsEnumerable().First();


            Console.WriteLine($" ID: {documents.id}; Name: {documents.name}");
            Console.WriteLine($" PostalCode: {documents.address.postalCode} ");
            Console.WriteLine();

            var addressUpdateMain = documents;
            Newtonsoft.Json.Linq.JObject addressUpdate = addressUpdateMain.address;
            addressUpdate["postalCode"] = "560002";
            addressUpdateMain.address = addressUpdate;

            //Console.WriteLine($" PostalCode: {documents.address.postalCode} ");

            var upd = await client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri("ADW", "stores", documents.id), addressUpdateMain);

            Console.WriteLine($"Number of RU's Used {upd.RequestCharge}");

        }

        private async static Task QueryDocumentsWithPaging(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Query Documents with Paging option <<<");
            Console.WriteLine();

            var sql = "SELECT * FROM c";
            var options = new FeedOptions { EnableCrossPartitionQuery = true, MaxItemCount=30 };

            var query = client.CreateDocumentQuery(mystoreColUri, sql, options).AsDocumentQuery();

            while (query.HasMoreResults)
            {
                var documents = await query.ExecuteNextAsync();
                Console.WriteLine($"Number of RU's Used {documents.RequestCharge}");
                foreach (var doc in documents)
                { 
                    Console.WriteLine($" Id: {doc.id}; Name: {doc.name};");
                    
                }
                Console.WriteLine($" count: {documents.Count};");
            }
            Console.WriteLine();
        }

        private static async Task UpsertDocument(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Update existing document. <<<");
            Console.WriteLine();

            Console.WriteLine("Querting documents by property filter");
            var sql = "SELECT * FROM c WHERE c.address.countryRegionName = 'India' and c.address.location.stateProvinceName = 'Bangalore'";
            //var option = new FeedOptions { EnableCrossPartitionQuery = true};


            var documents = client.CreateDocumentQuery(mystoreColUri, sql).AsEnumerable().First();


            Console.WriteLine($" ID: {documents.id}; Name: {documents.name}");
            Console.WriteLine($" PostalCode: {documents.address.postalCode} ");
            Console.WriteLine();

            var addressUpdateMain = documents;
            Newtonsoft.Json.Linq.JObject addressUpdate = addressUpdateMain.address;
            addressUpdate["postalCode"] = "560001";
            addressUpdateMain.address = addressUpdate;

            //Console.WriteLine($" PostalCode: {documents.address.postalCode} ");

            var upd = await client.UpsertDocumentAsync(mystoreColUri, addressUpdateMain);

            Console.WriteLine($"Number of RU's Used {upd.RequestCharge}");

        }

        private static async Task DeleteDcouments(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Deleting a document. <<<");
            Console.WriteLine();

            Console.WriteLine("Querting documents by property filter");
            var sql = "SELECT * FROM c WHERE c.address.countryRegionName = 'India' ";
            //var option = new FeedOptions { EnableCrossPartitionQuery = true};


            var documents = client.CreateDocumentQuery(mystoreColUri, sql).AsEnumerable().First();


            Console.WriteLine($" ID: {documents.id}; Name: {documents.name}");
            Console.WriteLine($" PostalCode: {documents.address.postalCode} ");
            Console.WriteLine();
            var docUri = UriFactory.CreateDocumentUri("ADW", "stores", documents.id);
            var ropt = new RequestOptions { PartitionKey = new PartitionKey("India") };


            var upd = await client.DeleteDocumentAsync(docUri,ropt);

            Console.WriteLine($"Number of RU's Used {upd.RequestCharge}");
        }
        #endregion


    }
}