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

    let makeSeq<'T> rtnObj = checkCollection<'T> rtnObj |> box
    let makeArray<'T> rtnObj = checkCollection<'T> rtnObj |> Array.ofSeq |> box
    let makeList<'T> rtnObj = checkCollection<'T> rtnObj |> List.ofSeq |> box
    let makeSet<'T when 'T : comparison > rtnObj = checkCollection<'T> rtnObj |> Set.ofSeq |> box

    // Test of collections
    // Driver returns a System.Collections.Generic.List`1[System.Object]
    let makeCollections(propTyp : Type) (rtnObj : obj) =
        if propTyp = typeof<seq<string>> then makeSeq<string> rtnObj
        elif propTyp = typeof<seq<int64>> then makeSeq<int64> rtnObj
        elif propTyp = typeof<seq<int>> then makeSeq<int> rtnObj
        elif propTyp = typeof<seq<float>> then makeSeq<float> rtnObj
        elif propTyp = typeof<seq<bool>> then makeSeq<bool> rtnObj
        elif propTyp = typeof<array<string>> then makeArray<string> rtnObj
        elif propTyp = typeof<array<int64>> then makeArray<int64> rtnObj
        elif propTyp = typeof<array<int>> then makeArray<int> rtnObj
        elif propTyp = typeof<array<float>> then makeArray<float> rtnObj
        elif propTyp = typeof<array<bool>> then makeArray<bool> rtnObj
        elif propTyp = typeof<list<string>> then makeList<string> rtnObj
        elif propTyp = typeof<list<int64>> then makeList<int64> rtnObj
        elif propTyp = typeof<list<int>> then makeList<int> rtnObj
        elif propTyp = typeof<list<float>> then makeList<float> rtnObj
        elif propTyp = typeof<list<bool>> then makeList<bool> rtnObj
        elif propTyp = typeof<Set<string>> then makeSet<string> rtnObj
        elif propTyp = typeof<Set<int64>> then makeSet<int64> rtnObj
        elif propTyp = typeof<Set<int>> then makeSet<int> rtnObj
        elif propTyp = typeof<Set<float>> then makeSet<float> rtnObj
        elif propTyp = typeof<Set<bool>> then makeSet<bool> rtnObj
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