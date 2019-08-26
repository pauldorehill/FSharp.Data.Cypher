namespace FSharp.Data.Cypher.Test.QueryBuilding

open FSharp.Data.Cypher
open FSharp.Data.Cypher.Test
open Xunit

module ``Primtive Types`` =

    [<Fact>]
    let ``Bool true`` () =
        cypher {
            RETURN true
        }
        |> Cypher.queryNonParameterized
        |> fun r -> Assert.Equal("RETURN True", r) //.net bool is caps
    
    [<Fact>]
    let ``Bool false`` () =
        cypher {
            RETURN false
        }
        |> Cypher.queryNonParameterized
        |> fun r -> Assert.Equal("RETURN False", r) //.net bool is caps
        
    [<Fact>]
    let ``int32`` () =
        cypher {
            RETURN 5
        }
        |> Cypher.queryNonParameterized
        |> fun r -> Assert.Equal("RETURN 5", r)
    
    [<Fact>]
    let ``int64`` () =
        cypher {
            RETURN 5L
        }
        |> Cypher.queryNonParameterized
        |> fun r -> Assert.Equal("RETURN 5", r)
        
    [<Fact>]
    let ``float`` () =
        cypher {
            RETURN 5.5
        }
        |> Cypher.queryNonParameterized
        |> fun r -> Assert.Equal("RETURN 5.5", r)
    
    [<Fact>]
    let ``string`` () =
        cypher {
            RETURN "EMU"
        }
        |> Cypher.queryNonParameterized
        |> fun r -> Assert.Equal("RETURN \"EMU\"", r)

module ``MATCH Statement`` =
    
    open ``Movie Graph As Records``

    [<Fact>]
    let ``Lots of Labels`` () =
        cypher {
            for ll in Graph.LotsOfLabels do
            MATCH ll
            RETURN ll
        }
        |> Cypher.queryNonParameterized
        |> fun r -> Assert.Equal("MATCH (ll:Label1:Label2:Label3:Label4) RETURN ll", r)
    
    [<Fact>]
    let ``Label with space`` () =
        cypher {
            for ls in Graph.LabelWithSpace do
            MATCH ls
            RETURN ls
        }
        |> Cypher.queryNonParameterized
        |> fun r -> Assert.Equal("MATCH (ls:`Label with spaces`) RETURN ls", r)

    [<Fact>]
    let ``Basic MATCH`` () =
        cypher {
            for m in Graph.Movie do
            MATCH m
            RETURN m
        }
        |> Cypher.queryNonParameterized
        |> fun r -> Assert.Equal("MATCH (m:Movie) RETURN m", r)

    [<Fact>]
    let ``Basic Ascii -- `` () =
        cypher {
            for m in Graph.Movie do
            for p in Graph.Person do
            MATCH (p -- m)
            RETURN m
        }
        |> Cypher.queryNonParameterized
        |> fun r -> Assert.Equal("MATCH (p:Person)--(m:Movie) RETURN m", r)

    [<Fact>]
    let ``Basic Ascii <-- `` () =
        cypher {
            for m in Graph.Movie do
            for p in Graph.Person do
            MATCH (p <-- m)
            RETURN m
        }
        |> Cypher.queryNonParameterized
        |> fun r -> Assert.Equal("MATCH (p:Person)<--(m:Movie) RETURN m", r)

    [<Fact>]
    let ``Basic Ascii --> `` () =
        cypher {
            for m in Graph.Movie do
            for p in Graph.Person do
            MATCH (p --> m)
            RETURN m
        }
        |> Cypher.queryNonParameterized
        |> fun r -> Assert.Equal("MATCH (p:Person)-->(m:Movie) RETURN m", r)

    [<Fact>]
    let ``Basic Ascii -| & |- `` () =
        cypher {
            for m in Graph.Movie do
            for a in Graph.ActedIn do
            for p in Graph.Person do
            MATCH (p -| a |- m)
            RETURN m
        }
        |> Cypher.queryNonParameterized
        |> fun r -> Assert.Equal("MATCH (p:Person)-[a:ACTED_IN]-(m:Movie) RETURN m", r)
    
    [<Fact>]
    let ``Basic Ascii <-| & |-> `` () =
        cypher {
            for m in Graph.Movie do
            for a in Graph.ActedIn do
            for p in Graph.Person do
            MATCH (p <-| a |-> m)
            RETURN m
        }
        |> Cypher.queryNonParameterized
        |> fun r -> Assert.Equal("MATCH (p:Person)<-[a:ACTED_IN]->(m:Movie) RETURN m", r)    

    [<Fact>]
    let ``Complex Ascii`` () =
        cypher {
            for m in Graph.Movie do
            for a in Graph.ActedIn do
            for d in Graph.Directed do
            for p in Graph.Person do
            MATCH (p -| a |-> m <-| d |- p)
            RETURN m
        }
        |> Cypher.queryNonParameterized
        |> fun r -> Assert.Equal("MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)<-[d:DIRECTED]-(p:Person) RETURN m", r)
    
    [<Fact>]
    let ``All Ascii`` () =
        cypher {
            for m in Graph.Movie do
            for a in Graph.ActedIn do
            for d in Graph.Directed do
            for p in Graph.Person do
            MATCH (p <-| a |-> m -- p --> p <-- p -| a |- m)
            RETURN m
        }
        |> Cypher.queryNonParameterized
        |> fun r -> 
            Assert.Equal("MATCH (p:Person)<-[a:ACTED_IN]->(m:Movie)--(p:Person)-->(p:Person)<--(p:Person)-[a:ACTED_IN]-(m:Movie) RETURN m", r)