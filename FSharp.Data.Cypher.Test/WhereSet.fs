namespace FSharp.Data.Cypher.Test.WHERE

open System
open FSharp.Data.Cypher
open Xunit

module ``Allowed operators`` =

    [<Fact>]
    let ``Equals =`` () =

        cypher {
            WHERE (42 = 42)
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("WHERE 42 = 42", q)

    [<Fact>]
    let ``Less than <`` () =

        cypher {
            WHERE (42 < 42)
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("WHERE 42 < 42", q)

    [<Fact>]
    let ``Less than or Equals <=`` () =

        cypher {
            WHERE (42 <= 42)
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("WHERE 42 <= 42", q)

    [<Fact>]
    let ``Greater than >`` () =

        cypher {
            WHERE (42 > 42)
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("WHERE 42 > 42", q)

    [<Fact>]
    let ``Greater than or Equals >=`` () =

        cypher {
            WHERE (42 >= 42)
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("WHERE 42 >= 42", q)

    [<Fact>]
    let ``Not Equal <>`` () =

        cypher {
            WHERE (42 <> 42)
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("WHERE 42 <> 42", q)

    [<Fact>]
    let ``Or`` () =

        cypher {
            WHERE (42 <> 42 || 42 <> 42)
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("WHERE 42 <> 42 OR 42 <> 42", q)

    [<Fact>]
    let ``And`` () =
        cypher {
            WHERE (42 <> 42 && 42 <> 42)
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("WHERE 42 <> 42 AND 42 <> 42", q)

module ``Option types`` =

    [<Fact>]
    let ``Some value`` () =

        cypher {
            WHERE (Some 42 = Some 42)
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("WHERE 42 = 42", q)

    [<Fact>]
    let ``None value`` () =

        cypher {
            WHERE (None = None)
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("WHERE null = null", q)

    [<Fact>]
    let ``Some None`` () =

        cypher {
            WHERE (Some 42 = None)
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("WHERE 42 = null", q)

namespace FSharp.Data.Cypher.Test.SET

open System
open FSharp.Data.Cypher
open Xunit

module ``Can build`` =
    
    open FSharp.Data.Cypher.Test.MATCH.Node
    
    [<Fact>]
    let ``Basic statement`` () =
        cypher {
            SET 42
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("SET 42", q)

    [<Fact>]
    let ``Tupled statement`` () =
        cypher {
            SET (42, 42, 42)
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("SET 42, 42, 42", q)

    open FSharp.Data.Cypher.Test.MATCH.Node

    [<Fact>]
    let ``Set a Node Label`` () =
        cypher {
            for n in Graph.NodeOfType do
            SET (n, n.Label)
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("SET n:NodeLabel", q)
    
    [<Fact>]
    let ``Set a tuple of Node Labels`` () =
        cypher {
            for n in Graph.NodeOfType do
            SET ((n, n.Label), (n, n.Label), (n, n.Label))
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("SET n:NodeLabel, n:NodeLabel, n:NodeLabel", q)
    
    [<Fact>]
    let ``Set a Node property : RHS`` () =
        cypher {
            for n in Graph.NodeOfType do
            SET (n.FloatValue = 5.5)
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("SET n.FloatValue = 5.5", q)
        
    [<Fact>]
    let ``Set a Node property : LHS`` () =
        cypher {
            for n in Graph.NodeOfType do
            SET (5.5 = n.FloatValue)
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("SET 5.5 = n.FloatValue", q)

namespace FSharp.Data.Cypher.Test.ON_CREATE_SET

open System
open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    [<Fact>]
    let ``Basic statement`` () =
        cypher {
            ON_CREATE_SET 42
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("ON CREATE SET 42", q)

namespace FSharp.Data.Cypher.Test.ON_MATCH_SET

open System
open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    [<Fact>]
    let ``Basic statement`` () =
        cypher {
            ON_MATCH_SET 42
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("ON MATCH SET 42", q)


namespace FSharp.Data.Cypher.Test.REMOVE

open System
open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    open FSharp.Data.Cypher.Test.MATCH.Node

    [<Fact>]
    let ``Basic statement`` () =
        cypher {
            for n in Graph.NodeOfType do
            REMOVE (n, n.Label)
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("REMOVE n:NodeLabel", q)

    [<Fact>]
    let ``Tupled statement`` () =
        cypher {
            for n in Graph.NodeOfType do
            REMOVE ((n, n.Label), (n, n.Label))
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("REMOVE n:NodeLabel, n:NodeLabel", q)

    [<Fact>]
    let ``Fails when not a remove statement`` () =
        let fmake () =
            cypher {
                for n in Graph.NodeOfType do
                REMOVE (n.FloatValue = 5.5)
            }
            |> Cypher.queryRaw
            |> ignore

        Assert.Throws(typeof<InvalidOperationException>, fmake)