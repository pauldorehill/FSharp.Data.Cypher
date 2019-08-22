# FSharp.Data.Cypher
Write type safe Cypher queries in F#

A computation expression that closely matches the standard Cypher syntax.

Great [article](http://tomasp.net/blog/2015/query-translation/)


```fsharp
// Define the graph
type Graph =
    static member Movie : Query<Movie> = NA
    static member ActedIn : Query<ActedIn> = NA
    static member Directed : Query<Directed> = NA
    static member Person : Query<Person> = NA

// Connect to database
let driver = GraphDatabase.Driver("bolt://localhost:7687", AuthTokens.None)

cypher {
    // Graph items in scope
    for m in Graph.Movie do
    for a in Graph.ActedIn do
    for p in Graph.Person do

    // Cypher query
    MATCH (p -| a |-> m)
    RETURN (p, m)
}
|> Cypher.write driver
|> Async.RunSynchronously
```