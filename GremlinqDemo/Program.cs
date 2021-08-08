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
            //Create Database
            var createResponse = await client.CreateDatabaseIfNotExistsAsync(DatabaseName);
            var db = createResponse.Database;
            //Create Container
            await db.CreateContainerIfNotExistsAsync(GraphName, PartitionKeyPath);
        }

        private static IGremlinQuerySource GetGremlinSource()
        {
            return GremlinQuerySource.g
                .ConfigureEnvironment(env => env
                    .UseModel(GraphModel
                        //Specify base classes for vertices(Vertex) and edges(Edge)
                        .FromBaseTypes<Vertex, Edge>(lookup => lookup.IncludeAssembliesOfBaseTypes()))
                    .UseCosmosDb(builder => builder
                        //Specify CosmosDb Gremlin endpoint URL, DB name and graph name. 
                        .At(new Uri(GremlinEndpointUrl), DatabaseName, GraphName)
                        //Specify CosmosDb access key
                        .AuthenticateBy(CosmosDbAuthKey)
                        ));
        }

        private static async Task CreateData(IGremlinQuerySource g)
        {
            //Create users
            var alice = await CreateUser(g, "Alice");
            var bob = await CreateUser(g, "Bob");
            var charlie = await CreateUser(g, "Charlie");
            var diana = await CreateUser(g, "Diana");

            //Create "Follows" edges
            await Follows(g, alice, bob);
            await Follows(g, bob, charlie);
            await Follows(g, bob, diana);
            await Follows(g, charlie, bob);

            //Create tweet
            var tweet1 = await CreateTweet(g, bob, "I love using gremlinq!");

            //Create "Liked" edges
            await Liked(g, alice, tweet1);
            await Liked(g, charlie, tweet1);

            await Retweeted(g, alice, tweet1);
        }

        static async Task<User> CreateUser(IGremlinQuerySource g, string name) =>
            await g
                //AddV means add vertex (in this case the vertex is a user
                .AddV(new User { Name = name })
                //FirstAsync will return the created user
                .FirstAsync();

        static async Task Follows(IGremlinQuerySource g, User follower, User followee) =>
            await g
                //Get the first user vertex
                .V(follower.Id)
                //Add an edge from the first user of type "Follows" to the second user
                .AddE<Follows>()
                .To(__ => __.V(followee.Id))
                //awaiting FirstAsync will ensure the "Follows" edge gets created
                .FirstAsync();

        static async Task Liked(IGremlinQuerySource g, User liker, Tweet tweet) =>
            await g
                .V(liker.Id)
                //Add an edge from the user of type "Liked" to the tweet
                .AddE<Liked>()
                .To(__ => __.V(tweet.Id))
                .FirstAsync();

        static async Task Retweeted(IGremlinQuerySource g, User retweeter, Tweet tweet) =>
            await g.V(retweeter.Id)
                .AddE<Retweeted>()
                .To(__ => __.V(tweet.Id))
                .FirstAsync();
        static async Task<Tweet> CreateTweet(IGremlinQuerySource g, User tweeter, string content) =>
            await g
                //Get the tweeter(user) vertex
                .V(tweeter.Id)
                // add a "Tweeted" edge from the user to a new tweet vertex created in the same query
                .AddE<Tweeted>()
                .To(__ => __.AddV(new Tweet { Content = content }))
                //Traverse from the "Tweeted" edge to the actual tweet
                //InV means go from the edge to the vertex it is going into
                .InV<Tweet>()
                .FirstAsync();

        private static async Task RunQueries(IGremlinQuerySource g)
        {
            var bob = await g
                .V()
                .OfType<User>()
                .Where(u => u.Name == "Bob")
                .FirstAsync();

            //We only created one tweet so we know tweet1 will be the first
            var tweet1 = await g
                .V()
                .OfType<Tweet>()
                .FirstAsync();

            await WhoFollowsBob(g,bob);
            await WhoLikedTweet1(g, tweet1);
            await WhoLikedAndRetweetedTweet1(g, tweet1);
            await WhoFollowsBobBack(g, bob);
        }
        private static async Task WhoFollowsBob(IGremlinQuerySource g, User bob)
        {
            var bobsFollowers = await g
                //Get all users with a Follows edge going "In" (pointing) to Bob
                .V(bob.Id)
                .In<Follows>()
                .OfType<User>()
                .ToArrayAsync();

            Console.WriteLine();
            Console.WriteLine("Users who follow Bob:");
            foreach (var user in bobsFollowers)
                Console.WriteLine(user.Name);
        }

        private static async Task WhoLikedTweet1(IGremlinQuerySource g, Tweet tweet1)
        {
            var tweetLikers = await g
                .V(tweet1.Id)
                .In<Liked>()
                .OfType<User>().ToArrayAsync();

            Console.WriteLine();
            Console.WriteLine("Users who liked Tweet1:");
            foreach (var user in tweetLikers)
                Console.WriteLine(user.Name);
        }

        private static async Task WhoLikedAndRetweetedTweet1(IGremlinQuerySource g, Tweet tweet1)
        {
            var likedAndRetweeted = await g
                //retrieve the list of users who liked the tweet
                .V(tweet1.Id)
                .In<Liked>()
                .OfType<User>()
                .Fold()
                //From the users who liked the tweet, get only the ones who also retweeted it.
                .As((__, likers) => __
                    .V(tweet1.Id)
                    .In<Retweeted>()
                    .OfType<User>()
                    .Where(retweeter => likers.Value.Contains(retweeter)))
                .ToArrayAsync();

            Console.WriteLine();
            Console.WriteLine("Users who liked and retweeted Tweet1:");
            foreach (var user in likedAndRetweeted)
                Console.WriteLine(user.Name);
        }

        private static async Task WhoFollowsBobBack(IGremlinQuerySource g, User bob)
        {
            var followsBobBack = await g
                //Find the users who are followed by Bob
                .V(bob.Id)
                .Out<Follows>()
                .OfType<User>()
                .Fold()
                //Filter to the users who follow Bob back
                .As((__, bobFollows) => __
                    .V(bob.Id)
                    .In<Follows>()
                    .OfType<User>()
                    .Where(bobsFollower => bobFollows.Value.Contains(bobsFollower)));

            Console.WriteLine();
            Console.WriteLine("Users who follow Bob and are followed by him :");
            foreach (var user in followsBobBack)
                Console.WriteLine(user.Name);
        }
        private static Task DeleteData(IGremlinQuerySource g)
        {
            throw new NotImplementedException();
        }

    }
}
