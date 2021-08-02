namespace GremlinqDemo.Models
{
    class Vertex
    {
        public string Id { get; set; }
        public string PartitionKey { get; } = "default";
    }
}