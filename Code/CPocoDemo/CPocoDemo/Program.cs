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

namespace CPocoDemo
{
    class Program
    {
        public static Uri dburi = UriFactory.CreateDatabaseUri("ADW");
        public static Uri mystoreColUri = UriFactory.CreateDocumentCollectionUri("ADW", "stores");

        // Read config
        private static readonly string endpoint = ConfigurationManager.AppSettings["CosmosDbEndpoint"];
        private static readonly string masterKey = ConfigurationManager.AppSettings["CosmosDbMasterKey"];

        //Reusable instance of DocumentClient which represents the connection to a DocumentDB endpoint
        private static DocumentClient client;

        private static void Main(string[] args)
        {

            ConnectionPolicy connectionPolicy = new ConnectionPolicy();
            connectionPolicy.ConnectionMode = ConnectionMode.Direct;
            connectionPolicy.ConnectionProtocol = Protocol.Tcp;

            // Set the read region selection preference order
            //connectionPolicy.PreferredLocations.Add(LocationNames.EastUS); // first preference
            //connectionPolicy.PreferredLocations.Add(LocationNames.NorthEurope); // second preference
            //connectionPolicy.PreferredLocations.Add(LocationNames.SoutheastAsia); // third preference

            var client = new DocumentClient(new Uri(endpoint), masterKey, connectionPolicy);

            try
            {
                ShowMenu();
                while (true)
                {
                    Console.WriteLine("Choose an option form the demo: ");
                    var input = Console.ReadLine();
                    var demoid = input.ToUpper().Trim();
                    if (demoid == "CDO")
                    {
                        CreateDocument(client).Wait();
                    }
                    else if (demoid == "QDO")
                    {
                        Customer result = QueryDocumentsByProperty(client);
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

CDO Create documents in mystore collection of ADW.
QDO Query documents in mystore collection of ADW by property filter.
Q Quit
");
        }

        private async static Task CreateDocument(DocumentClient client)
        {
            Console.WriteLine();
            Console.WriteLine(">>> Create Documents <<<");
            Console.WriteLine();

 
            var documentDefPoco = new Customer
            {
                Name = "New Customer 3",
                Address = new Address
                {
                    AddressType = "Main Office",
                    AddressLine1 = "123 Main Street",
                    Location = new Location
                    {
                        City = "Bangalore",
                        StateProvinceName = "Bangalore"
                    },
                    PostalCode = "560020",
                    CountryRegionName = "India"
                },
            };

            Document document3 = await client.CreateDocumentAsync(mystoreColUri, documentDefPoco);
            Console.WriteLine($"Created document {document3.Id} from typed object (POCO)");
            Console.WriteLine();
        }

        private static Customer QueryDocumentsByProperty(DocumentClient client)
        {
            //******************************************************************************************************************
            // NOTE: Operations like AsEnumerable(), ToList(), ToArray() will make as many trips to the database
            //       as required to fetch the entire result-set. Even if you set MaxItemCount to a smaller number. 
            //       MaxItemCount just controls how many results to fetch each trip. 
            //       If you don't want to fetch the full set of results, then use CreateDocumentQuery().AsDocumentQuery()
            //       For more on this please refer to the Queries project.
            //
            // NOTE: If you want to get the RU charge for a query you also need to use CreateDocumentQuery().AsDocumentQuery()
            //       and check the RequestCharge property of this IQueryable response
            //       Once again, refer to the Queries project for more information and examples of this
            //******************************************************************************************************************
            Console.WriteLine("Querying for a document using its City property");
            //To set max degree of parallelism and populate Query Metrics this will help get query execution stats related information.
            //var opt = new FeedOptions { MaxDegreeOfParallelism = 4, PopulateQueryMetrics = true };

            Customer CustIns = client.CreateDocumentQuery<Customer>(mystoreColUri)
                .Where(ci => ci.Address.CountryRegionName == "India" && ci.Address.Location.City == "Bangalore")
                .AsEnumerable().First();

            Console.WriteLine(CustIns.Name);

            return CustIns;
        }

       
    }
}
