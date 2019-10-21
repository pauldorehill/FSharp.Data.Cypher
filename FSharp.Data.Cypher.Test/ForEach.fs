namespace FSharp.Data.Cypher.Test.FOREACH

open System
open FSharp.Data.Cypher
open Xunit


module ``Can build`` =

    open FSharp.Data.Cypher.Test.MATCH.Node

    let basicStm count =
        List.replicate count "FOREACH (innerNode IN outerNodes | SET innerNode.IntValue = 5"
        |> String.concat " "
        |> fun s -> "MATCH (outerNodes) " + s + String.replicate count ")"

    [<Fact>]
    let ``Basic statement`` () =
        cypher {
            for outerNodes in Graph.NodeOfType do
            MATCH (Node outerNodes)
            FOREACH { for innerNode in outerNodes do SET (innerNode.IntValue = 5) }
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal(basicStm 1, q)

    [<Fact>]
    let ``Nested FOREACH`` () =
        cypher {
            for outerNodes in Graph.NodeOfType do
            MATCH (Node outerNodes)
            FOREACH {
                for innerNode in outerNodes do
                SET (innerNode.IntValue = 5)
                FOREACH {
                    for innerNode in outerNodes do
                    SET (innerNode.IntValue = 5)
                    FOREACH {
                        for innerNode in outerNodes do
                        SET (innerNode.IntValue = 5)
                        FOREACH {
                            for innerNode in outerNodes do
                            SET (innerNode.IntValue = 5)
                            FOREACH {
                                for innerNode in outerNodes do
                                SET (innerNode.IntValue = 5)
                            }
                        }
                    }
                }
            }
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal(basicStm 5, q)