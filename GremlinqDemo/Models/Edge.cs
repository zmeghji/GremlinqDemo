namespace GremlinqDemo.Models
{
    class Edge
    {
        public string Id { get; set; }
        public string PartitionKey { get;} = "default";
    }
}