namespace FSharp.Data.Cypher.Test.FOREACH

open System
open FSharp.Data.Cypher
open Xunit


module ``Can build`` =

    open FSharp.Data.Cypher.Test.MATCH.Node

    [<Fact>]
    let ``Basic statement`` () =
        cypher {
            for node in Graph.NodeOfType do
            FOREACH (ForEach())
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("FOREACH ", q)