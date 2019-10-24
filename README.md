# FSharp.Data.Cypher

A computation expression that very closely matches the standard Cypher syntax and allows typed Cypher queries in F#.
#### FSharp

```fsharp
cypher {
    for movie in Graph.Movie do
    for person in Graph.Person do
    for actedIn in Graph.ActedIn do

    MATCH (Node(person, person.Label) -- Rel(actedIn.Label) --> Node(movie, movie.Label))
    WHERE (movie.released < 1984 && person.born < Some 1960 )
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
#### Cypher
```
MATCH (person:Person)-[:ACTED_IN]->(movie:Movie)
WHERE movie.released < 1984 AND person.born < 1960
RETURN movie, person.name
LIMIT 1
```
Inspired by this great [article](http://tomasp.net/blog/2015/query-translation/).
## Still a work in progess!
Most clauses are avalible, working, and have tests. This documentation will also get improved... and a NuGet package.

## Contents
- [Differences with Cypher](#Differences-with-Cypher)
- [OPTIONAL MATCH and null](#OPTIONAL-MATCH-and-null)
- [FOREACH Clause](#FOREACH-Clause)
- [Supported Types](#Supported-Types)
- [Parameterization](#Parameterization)
- [Running a Query](#Running-a-Query)
- [Not yet supported](#Not-yet-supported)
- [Examples](#Examples)

## Differences With Cypher

The intent is to stay a close as possible to the cypher syntax: if you can write [cypher](https://neo4j.com/docs/cypher-refcard/current/) you are pretty much set.
Each `cypher` builder must start with 1 or more `for entity in Graph do` statements where where the `entity` needs to be of type `Node<'N>` where `'N :> IFSNode<'N>`, or `Rel<'R>` where `'R :> IFSRel<'R>`. For the example above that can look like:

```fsharp
type Movie =
    { title : string
      tagline : string option
      released : int }
    member _.Label = NodeLabel "Movie" // The node label is stored here for convenience
    interface IFSNode<Movie> // Currently these are just marker interfaces

type Person =
    { born : int option
      name : string }
    member _.Label = NodeLabel "Person"
    interface IFSNode<Person>

type ActedIn =
    { roles : string [] }
    member _.Label = RelLabel "ACTED_IN"
    interface IFSRel<ActedIn>

// Define the complete graph if you so wish
// Futher down the track a script that can generate all the graph types, or typeprovider could be plugged in
type Graph =
    static member Movie = Node<Movie>()
    static member Person = Node<Person>()

cypher {
    for movie in Graph.Movie do
    for person in Graph.Person do
    for actedIn in Node<ActedIn>() do // Or Simply wrap the type in the builder
}

```
#### Nodes
In cypher a node `(..)` consists of 3 optional parts: the `binding name`, a `label` or list of `labels`, and `property values`:

```(n:Label1 {property1: value1, property2: value2})```

In the F# builder match these 3 parts as parameters to a `Node()` constructor and we are done:

```fsharp
Node(n, Label1, { n with { property1 = value1; property2 = value 2 })
```

#### Relationships
In cypher a relationship `[..]` consists of 3 optional parts: the `binding name`, a `label` (or officially `Type`), and `property values`:

```[r:Label1 {property1: value1, property2: value2}]```

In the F# builder match these 3 parts as parameters to a `Rel()` constructor and we are done:

```fsharp
Rel(n, Label1, { n with { property1 = value1; property2 = value 2 })
```

#### Operators
Just double the number of  dashes:
Cypher | FSharp
--- | ---
`-` |`--`
`->`|`-->`
`<-`|`<--`
`()--()`|`Node() ---- Node()`
`()-[]->()`|`Node() -- Rel() --> Node()`

#### AS

To use name aliases (required for aggregating and other functions) there is the `AS<'T>()` type with a member to `AS : AS<'T> -> 'T`. The variable is defined up front then passed into the `AS` member:

```fsharp
cypher {
    let totalPeople = AS<int64>() // Define the variable
    for person in Graph.Person do
    MATCH (Node person)
    RETURN (count(person) .AS totalPeople) // Pass it to the AS method on the function
}
```
## OPTIONAL MATCH and null

Unfortunately from an F# perspective [null](https://neo4j.com/docs/cypher-manual/current/syntax/working-with-null/) is *'...is used to represent missing or undefined values...'* and this has the potential to all null into your program. With core types (`string, int, float etc...`):
- If null is encountered an `ArgumentNullException` will be thrown i.e. you won't be able to create a `record` with a null field, or return a core type of null
- If your `record` field  or return type is an `'T option`,  `null` will happily become `None`

OPTIONAL MATCH currently throws a spanner in the works since it will happily return `null` in place of node or relationship. When this is returned from the database the deserilizer will still be expecting a node, sees `null` and will intentionally throw `ArgumentNullException`. There is a nice solution coming for this where the OPTIONAL MATCH query will only work with a record option... coming soon.

## FOREACH Clause
[FOREACH](https://neo4j.com/docs/cypher-manual/current/clauses/foreach/) is a special type of clause in cypher... as such it is implemented as its own builder of type `Foreach`. It is then used inside the `cypher {  }` builder and `ForEach`'s can be nested within other `ForEach`'s
```fsharp
cypher {
    for person in Graph.Person do
    let people = AS<Person list>()
    MATCH (Node(person, person.Label))
    WITH (collect(person) .AS people, person)
    FOREACH { for p in people do SET (p, NodeLabel "ForEach") }
    RETURN person
}
```

## Supported Types

Currently you can only use F# record types, parameterless classes, or single case fieldless DUs. These complex types can be  built from the following core types:

`string, int32, int64, float, bool`

`option, seq, list, array, Set` of the above

Records should be the default since they allow the update syntax which is used when you want to bind to a node / relationship with the required properties. Parameterless classes & single case fieldless DUs are for when you have Node or Relationship with no properties:
```fsharp
type Follows =
    | NA
    member _.Label = RelLabel "FOLLOWS"
    interface IFSRel<Follows>

type Produced() =
    member _.Label = RelLabel "PRODUCED"
    interface IFSRel<Produced>
```
Some support for DUs will be implemented at a later date... classes are harder still.

## Parameterization

All queries are parameterized by default and sent as a single line of text. You are able to access both the raw and parameterized query by calling the `Query` member on `CypherBuilder` (and their equivalent multiline versions). For the example at the top:

```
MATCH (person:Person)-[:ACTED_IN]->(movie:Movie)
WHERE movie.released < 1984 AND person.born < 1960
RETURN movie, person.name
LIMIT 1
```
Is sent to the database as

```
MATCH (person:Person)-[:ACTED_IN]->(movie:Movie) WHERE movie.released < $p01 AND person.born < $p02 RETURN movie, person.name LIMIT $p00
```
with a `dictionary` of `[("p02", 1960); ("p01", 1984); ("p00", 1L)]`. The raw multiline version:

```
MATCH (person:Person)-[:ACTED_IN]->(movie:Movie)
WHERE movie.released < 1984 AND person.born < 1960
RETURN movie, person.name
LIMIT 1
```

## Running a Query

To run a query you need a `IDriver` instance from the standard `Neo4j.Driver` driver. Queries come in two flavours see the [docs](https://neo4j.com/docs/driver-manual/1.7/sessions-transactions/#driver-transactions) for more info.

#### Transaction Functions : `Cypher<'T> -> QueryResult<'T>`
These are automatically committed to the database

```fsharp
let driver = GraphDatabase.Driver( ... )
let results =
    cypher {
        for person in Node<Person>() do
        MATCH (Node person)
        RETURN (person)
    }
    |> Cypher.run driver

let results : Person [] = QueryResult.results result
let summary : IResultSummary = QueryResult.summary result
```

#### Explicit Transactions : `Cypher<'T> -> TransactionResult<'T>`
The query is sent to the database where it is run and returns the results - **however it is not committed** e.g. the database is not updated. The `TransactionResult` should then be either committed or rolled back to the database manually: this will need to happen before any subsequent queries.

```fsharp
let transactionResult =
    cypher {
        for person in Node<Person>() do
        MATCH (Node person)
        SET (person.name = "NewName")
        RETURN (person)
    }
    |> Cypher.Explicit.run driver

// Results available and have name = "NewName", but are not committed to database
// i.e. they would all still have their original names if you were to query them now
let results : Person [] = TransactionResult.results transactionResult

// Commit the results -> all names in database now set.
let commit : QueryResult<Person> = TransactionResult.commit transactionResult

// Rollback -> there is no change to the database
let rollBack : unit = TransactionResult.rollback transactionResult
```

## Not yet supported

#### Clauses
Constraints, query profiling, indexes, case expressions, stored proceedures.

#### Paths
No support for `Paths`, though I have something in mind.

#### Functions
Lots... not much done here.

#### Types
There are some other core types allowed with Neo4j e.g. date and times.

#### Operators
Mathematical, null, XOR, string matching, regex

#### Import
Nothing done yet

## Examples

TODO: Match the [Cypher Ref Card](https://neo4j.com/docs/cypher-refcard/current/)