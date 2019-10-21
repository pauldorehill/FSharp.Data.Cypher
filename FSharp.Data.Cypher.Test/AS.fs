namespace FSharp.Data.Cypher.Test.AS

open System
open FSharp.Data.Cypher
open FSharp.Data.Cypher.Functions.Aggregating
open Xunit

module ``Can build`` =

    open FSharp.Data.Cypher.Test.MATCH.Node

    let rtn = sprintf "RETURN %s(node%s) AS myVar"

    [<Fact>]
    let ``basic avg`` () =
        cypher {
            for node in Graph.NodeOfType do
            let myVar = AS<float>()
            RETURN (avg(node.FloatValue) .AS myVar)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal(rtn "avg" ".FloatValue", q)

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
    
    [<Fact>]
    let ``basic count`` () =
        cypher {
            for node in Graph.NodeOfType do
            let myVar = AS<int64>()
            RETURN (count(node) .AS myVar)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal(rtn "count" "", q)

    [<Fact>]
    let ``max node property`` () =
        cypher {
            for node in Graph.NodeOfType do
            let myVar = AS<float>()
            RETURN (max(node.FloatValue) .AS myVar)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal(rtn "max" ".FloatValue", q)

    [<Fact>]
    let ``min node property`` () =
        cypher {
            for node in Graph.NodeOfType do
            let myVar = AS<float>()
            RETURN (min(node.FloatValue) .AS myVar)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal(rtn "min" ".FloatValue", q)

    [<Fact>]
    let ``sum node property`` () =
        cypher {
            for node in Graph.NodeOfType do
            let myVar = AS<float>()
            RETURN (sum(node.FloatValue) .AS myVar)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal(rtn "sum" ".FloatValue", q)