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

    [<Fact>]
    let ``Remove list`` () =
        cypher {
            for n in Graph.NodeOfType do
            REMOVE (n, [ n.Label; n.Label])
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("REMOVE n:NodeLabel", q)
