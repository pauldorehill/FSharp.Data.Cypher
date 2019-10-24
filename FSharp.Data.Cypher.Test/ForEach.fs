namespace FSharp.Data.Cypher.Test.FOREACH

open System
open FSharp.Data.Cypher
open Xunit


module ``Can build`` =

    open FSharp.Data.Cypher.Test.MATCH.Node
    open FSharp.Data.Cypher.Functions
    open Aggregating

    let basicStm count =
        List.replicate count "FOREACH (innerNode IN nodeList | SET innerNode.IntValue = 5"
        |> String.concat " "
        |> fun s -> "WITH collect(outerNodes) AS nodeList " + s + String.replicate count ")"

    [<Fact>]
    let ``Basic statement`` () =
        cypher {
            for outerNodes in Graph.NodeOfType do
            let nodeList = AS<NodeType list>()
            WITH (collect(outerNodes) .AS nodeList)
            FOREACH { for innerNode in nodeList do SET (innerNode.IntValue = 5) }
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal(basicStm 1, q)

    [<Fact>]
    let ``Nested FOREACH`` () =
        cypher {
            for outerNodes in Graph.NodeOfType do
            let nodeList = AS<NodeType list>()
            WITH (collect(outerNodes) .AS nodeList)
            FOREACH { 
                for innerNode in nodeList do
                SET (innerNode.IntValue = 5) 
                FOREACH { 
                    for innerNode in nodeList do
                    SET (innerNode.IntValue = 5)
                    FOREACH { 
                        for innerNode in nodeList do
                        SET (innerNode.IntValue = 5) 
                        FOREACH { 
                            for innerNode in nodeList do
                            SET (innerNode.IntValue = 5) 
                            FOREACH { 
                                for innerNode in nodeList do
                                SET (innerNode.IntValue = 5) 
                            }
                        }
                    }
                }
            }
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal(basicStm 5, q)