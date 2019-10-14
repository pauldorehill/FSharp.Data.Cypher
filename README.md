# FSharp.Data.Cypher

A computation expression that closely matches the standard Cypher syntax and allows type safe Cypher queries in F#

```fsharp
cypher {
    for movie in Graph.Movie do
    for person in Graph.Person do
    for actedIn in Graph.ActedIn do

    MATCH (Node(person, person.Label) -- Rel(actedIn.Label) --> Node (movie, movie.Label))
    WHERE (movie.released < 1984 && person.born < Some 1960)
    RETURN (movie, person.name)
    LIMIT 1L
}
|> Cypher.run driver
|> QueryResult.results
// val it : (Movie * string) [] =
//   [|({ title = "One Flew Over the Cuckoo's Nest"
//        tagline = Some "If he's crazy, what does that make you?"
//        released = 1975 }, "Danny DeVito")|]

```
Inspired by this great [article](http://tomasp.net/blog/2015/query-translation/)
## Still a work in progess!

## Contents
- [Differences with Cypher](#Differences-with-Cypher)
- [Supported Types](#Supported-Types)
- [Supported Clauses](#Supported-Clauses)
- [Parameterization](#Parameterization)
- [Examples](#Examples)

## Differences With Cypher

The intent is to stay a close as possible to the cypher syntax: so if you can write cypher you are pretty much set.

#### `Node(...)` vs `(..)`
#### `Rel(...)` vs `[..]`
#### Operators
##### `--`
##### `-->`
##### `<--`

## Supported Types

Currently you can only use F# record types, or parameterless classes built from the following:

`string, int32, int64, float, bool`

`option, seq, list, array, Set` of the above

## Supported Clauses

## Parameterization

All queries are paramterized by default. You are able to access both the raw and parameterized query, for the example at the top:

```
MATCH (person:Person)-[:ACTED_IN]->(movie:Movie)
WHERE movie.released < 1984 AND person.born < 1960
RETURN movie, person.name
LIMIT 1
```
Is sent to the database as

```
MATCH (person:Person)-[:ACTED_IN]->(movie:Movie)
WHERE movie.released < $step3param1 AND person.born < $step3param2
RETURN movie, person.name
LIMIT $step1param1
```

## Examples


```fsharp
// Define a Node by assigning IFSNode
type Movie =
    { title : string
      tagline : string option
      released : int }
    interface IFSNode
    member _.Label = NodeLabel "Movie"

// Define a Node by assigning IFSNode
type Person =
    { born : int option
      name : string }
    interface IFSNode
    member _.Label = NodeLabel "Person"

// Define a Relationship by assigning IFSRelationship
type ActedIn =
    { roles : string [] }
    interface IFSRelationship
    member _.Label = RelLabel "ACTED_IN"

// Define a graph
type Graph =
    static member Movie : Query<Movie> = NA
    static member Person : Query<Person> = NA
    static member ActedIn : Query<ActedIn> = NA

// Create driver instance
let driver = GraphDatabase.Driver("bolt://localhost:7687", AuthTokens.None)

// Run a query

// MATCH (person:Person)-[:ACTED_IN]->(movie:Movie)
// WHERE movie.released < 1984 AND person.born < 1960
// RETURN movie, person.name
// LIMIT 1

let results =
    cypher {
        for movie in Graph.Movie do
        for person in Graph.Person do
        for actedIn in Graph.ActedIn do

        MATCH (Node(person, person.Label) -- Rel(actedIn.Label) --> Node (movie, movie.Label))
        WHERE (movie.released < 1984 && person.born < Some 1960)
        RETURN (movie, person.name)
        LIMIT 1L

    }
    |> Cypher.run driver
    |> QueryResult.results

// val results : (Movie * string) [] =
//   [|({ title = "One Flew Over the Cuckoo's Nest"
//        tagline = Some "If he's crazy, what does that make you?"
//        released = 1975 }, "Danny DeVito")|]
```