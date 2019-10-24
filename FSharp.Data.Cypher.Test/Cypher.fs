namespace FSharp.Data.Cypher.Test.MovieGraph


open System
open Neo4j.Driver
open FSharp.Data.Cypher
open FSharp.Data.Cypher.Test
open Xunit
open GraphDomains.MovieGraph

module ``Multi Line Queries`` =
    
    open FSharp.Data.Cypher.Functions
    open Aggregating

    [<Fact>]
    let ``Can make multiline`` () =
        cypher {
            for person in Graph.Person do
            MATCH (Node (person, person.Label))
            WITH (person)
            RETURN person
        }
        |> Cypher.query
        |> Query.rawMultiline
        |> fun x -> 
            Assert.Equal(
                [ "MATCH (person:Person)"
                  "WITH person"
                  "RETURN person" ]
                |> String.concat Environment.NewLine, x)
    
    [<Fact>]
    let ``Can make multiline parameterized`` () =
        let r =
            cypher {
                for person in Graph.Person do
                MATCH (Node (person, person.Label))
                SET (person.born = None)
                SET (person.born = Some 2000, person.name = "THOR")
                RETURN person
            }

        let rtn =
            [ "MATCH (person:Person)"
              "SET person.born = $p02"
              "SET person.born = $p00, person.name = $p01"
              "RETURN person" ]
            |> String.concat Environment.NewLine

        let ps : ParameterList =
            [ "p02", None
              "p01", Some (box "THOR")
              "p00", Some (box 2000) ]

        Assert.Equal(rtn, r.Query.ParameterizedMultiline)
        Assert.Equal(ps, Seq.ofList r.Query.Parameters)
    
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
                [ "MATCH (person:Person)"
                  "WITH collect(person) AS people, person"
                  "FOREACH (p IN people |"
                  "    SET p:ForEach"
                  "    SET p:ForEach)"
                  "RETURN person" ]
                |> String.concat Environment.NewLine, x)
    [<Fact>]
    let ``Nested FOREACH correctly indented`` () =
        cypher {
            for person in Graph.Person do
            let people = AS<Person list>()
            MATCH (Node (person, person.Label))
            WITH (collect(person) .AS people, person)
            FOREACH { 
                for p in people do 
                SET (p, NodeLabel "ForEach") 
                SET (p, NodeLabel "ForEach")
                FOREACH { 
                    for p in people do 
                    SET (p, NodeLabel "ForEach") 
                    SET (p, NodeLabel "ForEach")
                    FOREACH { 
                        for p in people do 
                        SET (p, NodeLabel "ForEach") 
                        SET (p, NodeLabel "ForEach") 
                    }
                }
            }
            RETURN person
        }
        |> Cypher.query
        |> Query.rawMultiline
        |> fun x -> 
            Assert.Equal(
                [ "MATCH (person:Person)"
                  "WITH collect(person) AS people, person"
                  "FOREACH (p IN people |"
                  "    SET p:ForEach"
                  "    SET p:ForEach"
                  "    FOREACH (p IN people |"
                  "        SET p:ForEach"
                  "        SET p:ForEach"
                  "        FOREACH (p IN people |"
                  "            SET p:ForEach"
                  "            SET p:ForEach)))" 
                  "RETURN person" ]
                |> String.concat Environment.NewLine, x)