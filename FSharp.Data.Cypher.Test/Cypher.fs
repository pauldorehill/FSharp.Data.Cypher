namespace FSharp.Data.Cypher.Test.MovieGraph

open Neo4j.Driver
open FSharp.Data.Cypher
open FSharp.Data.Cypher.Test
open Xunit
open GraphDomains.MovieGraph

module ``Multi Line Queries`` =
    
    open FSharp.Data.Cypher.Functions
    open Aggregating

    [<Fact>]
    let ``Is multiline`` () =
        cypher {
            for person in Graph.Person do
            //let people = AS<Person list>()
            MATCH (Node (person, person.Label))
            WITH (person)
            RETURN person
        }
        |> Cypher.query
        |> Query.rawMultiline
        |> fun x -> 
            Assert.Equal(
               "MATCH (person:Person)\r\n\
                WITH person\r\n\
                RETURN person", x)
    
    [<Fact>]
    let ``FOREACH correctly indented`` () =
        cypher {
            for person in Graph.Person do
            let people = AS<Person list>()
            MATCH (Node (person, person.Label))
            WITH (collect(person) .AS people, person)
            FOREACH { 
                for p in people do 
                SET (p, NodeLabel "ForEach") 
                SET (p, NodeLabel "ForEach") 
            }
            RETURN person
        }
        |> Cypher.query
        |> Query.rawMultiline
        |> fun x -> 
            Assert.Equal(
             """MATCH (person:Person)
                WITH collect(person) AS people, person
                FOREACH (p IN people |
                    SET p:ForEach
                    SET p:ForEach)
                RETURN person""", x)

    

