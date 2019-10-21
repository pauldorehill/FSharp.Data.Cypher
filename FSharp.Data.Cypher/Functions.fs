namespace FSharp.Data.Cypher

[<NoComparison; NoEquality>]
type AS<'T>() =
    member _.AS (x : AS<'T>) : 'T = invalidOp "AS.AS should never be called"
    member _.Value : 'T = invalidOp "AS.Value should never be called"

namespace FSharp.Data.Cypher.Functions

open FSharp.Data.Cypher

module Aggregating =
    
    let inline avg (x : 'T) = 
        List.average [x] |> ignore
        AS<'T>()

    let inline collect (x : 'T) = AS<'T list>()

    let inline count (x : 'T) = AS<int64>()
    
    let inline max (x : 'T) = //TODO: check type returned from Neo4j
        List.max [x] |> ignore
        AS<'T>()

    let inline min (x : 'T) = 
        List.min [x] |> ignore
        AS<'T>()

    let inline sum (x : 'T) = 
        List.sum [x] |> ignore
        AS<'T>()    