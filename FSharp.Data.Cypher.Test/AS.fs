namespace FSharp.Data.Cypher.Test.AS

open System
open FSharp.Data.Cypher
open Xunit
open AggregatingFunctions

module ``Can build`` =

    open FSharp.Data.Cypher.Test.MATCH.Node

    let rtn = sprintf "RETURN %s(node%s) AS myVar"

    [<Fact>]
    let ``count`` () =
        cypher {
            for node in Graph.NodeOfType do
            let myVar = AS<int64>()
            //MATCH (Node node)
            RETURN (count(node) .AS myVar)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal(rtn "count" "", q)
    
    [<Fact>]
    let ``collect nodes`` () =
        cypher {
            for node in Graph.NodeOfType do
            let myVar = AS<NodeType list>()
            RETURN (collect(node) .AS myVar)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal(rtn "collect" "", q)
        
    [<Fact>]
    let ``collect node property`` () =
        cypher {
            for node in Graph.NodeOfType do
            let myVar = AS<string list>()
            RETURN (collect(node.StringValue) .AS myVar)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal(rtn "collect" ".StringValue", q)