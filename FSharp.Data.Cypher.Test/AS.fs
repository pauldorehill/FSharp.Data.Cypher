namespace FSharp.Data.Cypher.Test.AS

open System
open FSharp.Data.Cypher
open Xunit
open AggregatingFunctions

module ``Can build`` =

    open FSharp.Data.Cypher.Test.MATCH.Node

    let rtn = sprintf "RETURN %s(node) AS myVar"

    [<Fact>]
    let ``count`` () =
        cypher {
            for node in Graph.NodeOfType do
            let myVar = AS<int64>()
            //MATCH (Node node)
            RETURN (count(node) .AS myVar)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal(rtn "count", q)
    
    [<Fact>]
    let ``collect`` () =
        cypher {
            for node in Graph.NodeOfType do
            let myVar = AS<NodeType list>()
            //MATCH (Node node)
            RETURN (collect(node) .AS myVar)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal(rtn "collect", q)