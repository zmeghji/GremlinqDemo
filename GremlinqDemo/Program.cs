using ExRam.Gremlinq.Core;
using GremlinqDemo.Models;
using Cosmos = Microsoft.Azure.Cosmos;
using System;
using System.Threading.Tasks;
using System.Linq;

namespace GremlinqDemo
{
    class Program
    {
        static private string DatabaseName = "twitter";
        static private string GraphName = "twitter";
        static private string PartitionKeyPath = "/PartitionKey";
        static private string CosmosConnectionString = "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        static private string GremlinEndpointUrl = "ws://localhost:8901/";
        static private string CosmosDbAuthKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

        static async Task Main(string[] args)
        {
            await CreateDb();

            IGremlinQuerySource g = GetGremlinSource();

            await CreateData(g);

            await RunQueries(g);

            await DeleteData(g);
        }

        private static async Task CreateDb()
        {
            var client = new Cosmos.CosmosClient(CosmosConnectionString);
            var createResponse = await client.CreateDatabaseIfNotExistsAsync(DatabaseName);
            var db = createResponse.Database;
            await db.CreateContainerIfNotExistsAsync(GraphName, PartitionKeyPath);
        }

        private static IGremlinQuerySource GetGremlinSource()
        {
            throw new NotImplementedException();
        }

        private static async Task CreateData(IGremlinQuerySource g)
        {
            throw new NotImplementedException();
        }

        private static async Task RunQueries(IGremlinQuerySource g)
        {
            throw new NotImplementedException();
        }

        private static Task DeleteData(IGremlinQuerySource g)
        {
            throw new NotImplementedException();
        }

    }
}
