namespace FSharp.Data.Cypher.Test.FOREACH

open System
open FSharp.Data.Cypher
open Xunit


module ``Can build`` =

    open FSharp.Data.Cypher.Test.MATCH.Node

    [<Fact>]
    let ``Basic statement`` () =
        cypher {
            for outerNodes in Graph.NodeOfType do
            MATCH (Node outerNodes)
            FOREACH { for innerNode in outerNodes do SET (innerNode.IntValue = 5) }
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("MATCH (outerNodes) FOREACH (innerNode IN outerNodes | SET innerNode.IntValue = 5)", q)