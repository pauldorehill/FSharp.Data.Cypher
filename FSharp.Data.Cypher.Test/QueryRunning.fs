namespace FSharp.Data.Cypher.Test.QueryRunning

open System.Collections
open FSharp.Data.Cypher
open FSharp.Data.Cypher.Test
open Xunit

module ``Primtive Types`` =

    let spoofDic =  
        [ "True", box true // dotnet use caps in serialization
          "False", box false
          "5", box 5L // DB returns int64
          "7", box 7L
          "5.5", box 5.5
          "\"EMU\"", box "EMU" ]
        |> Map.ofList
        |> Generic.Dictionary

    [<Fact>]
    let ``Bool true`` () =
        cypher {
            RETURN true
        }
        |> Cypher.run driver
        |> CypherResult.results
        |> Seq.head
        |> fun r -> Assert.Equal(true, r)
    
    [<Fact>]
    let ``Bool false`` () =
        cypher {
            RETURN false
        }
        |> Cypher.run driver
        |> CypherResult.results
        |> Seq.head
        |> fun r -> Assert.Equal(false, r)
        
    [<Fact>]
    let ``int32`` () =
        cypher {
            RETURN 5
        }
        |> Cypher.run driver
        |> CypherResult.results
        |> Seq.head
        |> fun r -> Assert.Equal(5, r)

    [<Fact>]
    let ``int64`` () =
        cypher {
            RETURN 5L
        }
        |> Cypher.run driver
        |> CypherResult.results
        |> Seq.head
        |> fun r -> Assert.Equal(5L, r)

    [<Fact>]
    let ``float`` () =
        cypher {
            RETURN 5.5
        }
        |> Cypher.run driver
        |> CypherResult.results
        |> Seq.head
        |> fun r -> Assert.Equal(5.5, r)

    [<Fact>]
    let ``string`` () =
        cypher {
            RETURN "EMU"
        }
        |> Cypher.run driver
        |> CypherResult.results
        |> Seq.head
        |> fun r -> Assert.Equal("EMU", r)

    let ``Tuple of all`` = 
        cypher {
            RETURN (true, false, 5, 7L, 5.5, "EMU")
        }
        |> Cypher.run driver
        |> CypherResult.results
        |> Seq.head
        |> fun r -> Assert.Equal((true, false, 5, 7L, 5.5, "EMU"), r)

module ``All Types Allowed on Record`` =
    
    let spoofDic =  
        [ "allAllowedSome", box ``All Allowed``.allAllowedSome
          "allAllowedNone", box ``All Allowed``.allAllowedNone 
          "nonCollections", box ``All Allowed``.nonCollections ]
        |> Map.ofList
        |> Generic.Dictionary
    
    type Graph =
        static member AllAllowed : Query<AllAllowed> = NA

    [<Fact>]
    let ``Can do all when return Some`` ()=
        cypher {
            for allAllowedSome in Graph.AllAllowed do

            MATCH (allAllowedSome)
            RETURN (allAllowedSome)
        }
        |> Cypher.spoof spoofDic
        |> Assert.IsType<AllAllowed>

    [<Fact>]
    let ``Can do all when return None`` ()=
        cypher {
            for allAllowedNone in Graph.AllAllowed do

            MATCH (allAllowedNone)
            RETURN (allAllowedNone)
        }
        |> Cypher.spoof spoofDic
        |> Assert.IsType<AllAllowed>

    [<Fact>]
    let ``Can do all when collections are single items`` ()=
        cypher {
            for nonCollections in Graph.AllAllowed do

            MATCH (nonCollections)
            RETURN (nonCollections)
        }
        |> Cypher.spoof spoofDic
        |> Assert.IsType<AllAllowed>

module ``Complex Queries with Record Types`` = 
    
    open ``Movie Graph As Records``

    [<Fact>]
    let ``Can do string, int deserialization`` () =
        cypher {
            for m in Graph.Movie do
            for a in Graph.ActedIn do
            for d in Graph.Directed do
            for p in Graph.Person do
            MATCH (p -| a |-> m <-| d |- p)
            RETURN (m.title, m.released)
        }
        |> Cypher.run driver
        |> CypherResult.results
        |> Seq.head
        |> fun (s, i) -> 
            (Assert.IsType<string> s) |> ignore
            (Assert.IsType<int> i)
    
    [<Fact>]
    let ``Can do basic Type deserialization`` () =
        cypher {
            for m in Graph.Movie do
            for a in Graph.ActedIn do
            for d in Graph.Directed do
            for p in Graph.Person do
            MATCH (p -| a |-> m <-| d |- p)
            RETURN (m, p, a, d)
        }
        |> Cypher.run driver
        |> CypherResult.results
        |> Seq.head
        |> fun (m, p, a, d) -> 
            Assert.IsType<Movie> m |> ignore
            Assert.IsType<ActedIn> a |> ignore
            Assert.IsType<Directed> d |> ignore
            Assert.IsType<Person> p