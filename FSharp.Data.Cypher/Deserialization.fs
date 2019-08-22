namespace FSharp.Data.Cypher

open System
open System.Reflection
open FSharp.Reflection
open Neo4j.Driver.V1

module Deserialization =
    
    let isOption (pi : PropertyInfo) =
        pi.PropertyType.IsGenericType && pi.PropertyType.GetGenericTypeDefinition() = typedefof<Option<_>>

    let makeOption (v : obj) =
        if isNull v then box None 
        else
            match v with
            | :? string as s -> box (Some s)
            | :? int64 as i -> box (Some i)
            | :? int as i -> box (Some i)
            | :? bool as b -> box (Some b)
            | :? float as f -> box (Some f)
            | _ -> invalidArg (v.GetType().Name) "An unhandled option type was found: "

    let checkCollection<'T> (rtnObj : obj) = 
        if rtnObj.GetType() = typeof<Collections.Generic.List<obj>> then
            rtnObj 
            :?> Collections.Generic.List<obj>
            |> Seq.cast<'T>
        else 
            rtnObj :?> 'T |> Seq.singleton

    // Test of collections
    // Driver returns a System.Collections.Generic.List`1[System.Object]
    let makeCollections(propTyp : Type) (rtnObj : obj) =
        if propTyp = typeof<seq<string>> then checkCollection<string> rtnObj |> box
        elif propTyp = typeof<seq<int64>> then checkCollection<int64> rtnObj |> box
        elif propTyp = typeof<seq<int>> then checkCollection<int> rtnObj |> box
        elif propTyp = typeof<seq<float>> then checkCollection<float> rtnObj |> box
        elif propTyp = typeof<seq<bool>> then checkCollection<bool> rtnObj |> box
        elif propTyp = typeof<array<string>> then checkCollection<string> rtnObj |> Array.ofSeq |> box
        elif propTyp = typeof<array<int64>> then checkCollection<int64> rtnObj |> Array.ofSeq |> box
        elif propTyp = typeof<array<int>> then checkCollection<int> rtnObj |> Array.ofSeq |> box
        elif propTyp = typeof<array<float>> then checkCollection<float> rtnObj |> Array.ofSeq |> box
        elif propTyp = typeof<array<bool>> then checkCollection<bool> rtnObj |> Array.ofSeq |> box
        elif propTyp = typeof<list<string>> then checkCollection<string> rtnObj |> List.ofSeq |> box
        elif propTyp = typeof<list<int64>> then checkCollection<int64> rtnObj |> List.ofSeq |> box
        elif propTyp = typeof<list<int>> then checkCollection<int> rtnObj |> List.ofSeq |> box
        elif propTyp = typeof<list<float>> then checkCollection<float> rtnObj |> List.ofSeq |> box
        elif propTyp = typeof<list<bool>> then checkCollection<bool> rtnObj |> List.ofSeq |> box
        elif propTyp = typeof<Set<string>> then checkCollection<string> rtnObj |> Set.ofSeq |> box
        elif propTyp = typeof<Set<int64>> then checkCollection<int64> rtnObj |> Set.ofSeq |> box
        elif propTyp = typeof<Set<int>> then checkCollection<int> rtnObj |> Set.ofSeq |> box
        elif propTyp = typeof<Set<float>> then checkCollection<float> rtnObj |> Set.ofSeq |> box
        elif propTyp = typeof<Set<bool>> then checkCollection<bool> rtnObj |> Set.ofSeq |> box
        else 
            propTyp.Name
            |> sprintf "Unsupported collection type: %s"
            |> invalidOp

    // TODO: Fix nulls, perform check on allowed types
    let fixTypes (propTyp : Type) (rtnObj : obj) =
        if propTyp = typeof<string> then 
            let s = rtnObj :?> string
            if isNull s then box String.Empty else rtnObj

        elif propTyp = typeof<int32> then 
            let i = rtnObj :?> int64
            if i <= int64 Int32.MaxValue then i |> int |> box
            else InvalidCastException "Can't convert int64 to int32. Value returned is greater than Int32.MaxValue" |> raise

        elif not(isNull (propTyp.GetInterface "IEnumerable")) then makeCollections propTyp rtnObj
        else rtnObj
    
    let deserialize (typ : Type) (entity : IEntity) = 
        typ.GetProperties()
        |> Array.map (fun pi ->
            match entity.Properties.TryGetValue pi.Name with
            | true, v -> 
                let v = fixTypes pi.PropertyType v
                if isOption pi then makeOption v else v
            | _ ->
                // If its an option and not there set to None else its an error
                if isOption pi 
                then box None 
                else invalidArg pi.Name "Could not deserialize node - the required value was not found on the database node: ")
      
    // Look into FSharpValue.PreComputeRecordConstructor - is it faster?
    // https://codeblog.jonskeet.uk/2008/08/09/making-reflection-fly-and-exploring-delegates/
    let toRecord (typ : Type) (entity : IEntity) = FSharpValue.MakeRecord(typ, deserialize typ entity)
    
    // Support parameterless constuctors
    let toClass (typ : Type) (entity : IEntity) = 
        let obs = deserialize typ entity
        if Array.isEmpty obs 
        then Activator.CreateInstance(typ)
        else Activator.CreateInstance(typ, obs)