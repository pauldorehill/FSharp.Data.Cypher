namespace FSharp.Data.Cypher.Test.UNWIND
open System
open FSharp.Data.Cypher
open FSharp.Data.Cypher.Functions
open Aggregating
open Xunit

module ``Can build`` =

    open FSharp.Data.Cypher.Test.MATCH.Node

    [<Fact>]
    let ``From collect`` () =
        cypher {
            for node in Graph.NodeOfType do
            let myVar = AS<NodeType list>()
            UNWIND (collect(node) .AS myVar)
        }
        |> Cypher.queryRaw
        |> fun q -> Assert.Equal("UNWIND collect(node) AS myVar", q)

    //[<Fact>]
    //let ``From list creation`` () =
    //    cypher {
    //        for node in Graph.NodeOfType do
    //        let myVar = AS<NodeType list>()
    //        UNWIND ([ 1 .. 2 ])
    //    }
    //    |> Cypher.queryNonParameterized
    //    |> fun q -> Assert.Equal("UNWIND collect(node) AS myVar", q)
