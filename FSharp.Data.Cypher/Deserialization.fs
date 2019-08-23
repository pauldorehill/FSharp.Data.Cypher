﻿namespace FSharp.Data.Cypher

open System
open System.Reflection
open FSharp.Reflection
open Neo4j.Driver.V1

module Deserialization =

    let isOption (typ : Type) = typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<Option<_>>

    let hasInterface (typ : Type) (name : string) = typ.GetInterface name |> isNull |> not

    let checkCollection<'T> (rtnObj : obj) = 
        if isNull rtnObj then Seq.empty
        elif rtnObj.GetType() = typeof<Collections.Generic.List<obj>> then
            rtnObj 
            :?> Collections.Generic.List<obj>
            |> Seq.cast<'T>
        else 
            rtnObj :?> 'T |> Seq.singleton // Fix up case when database isn't a collection

    let makeSeq<'T> rtnObj = checkCollection<'T> rtnObj |> box
    let makeArray<'T> rtnObj = checkCollection<'T> rtnObj |> Array.ofSeq |> box
    let makeList<'T> rtnObj = checkCollection<'T> rtnObj |> List.ofSeq |> box
    let makeSet<'T when 'T : comparison> rtnObj = checkCollection<'T> rtnObj |> Set.ofSeq |> box

    let fixInt (obj : obj) =
        let i = obj :?> int64
        if i <= int64 Int32.MaxValue then i |> int |> box
        else InvalidCastException "Can't convert int64 to int32. Value returned is greater than Int32.MaxValue" |> raise

    let makeOption<'T> (obj : obj) = if isNull obj then box None else obj :?> 'T |> Some |> box

    // TODO: this needs to be tidied up
    let fixTypes (name : string) (localType : Type) (dbObj : obj) =

        let nullCheck (obj : obj) = 
            if isNull obj then
                localType.Name
                |> sprintf "A null object was returned for %s on type %A" name
                |> ArgumentNullException
                |> raise
            else obj

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

        if localType = typeof<string> then nullCheck dbObj
        elif localType = typeof<option<string>> then makeOption<string> dbObj
        elif localType = typeof<int64> then nullCheck dbObj
        elif localType = typeof<option<int64>> then makeOption<int64> dbObj
        elif localType = typeof<int32> then dbObj |> nullCheck |> fixInt
        elif localType = typeof<option<int32>> then makeOption<int> dbObj
        elif localType = typeof<float> then nullCheck dbObj
        elif localType = typeof<option<float>> then makeOption<float> dbObj
        elif localType = typeof<bool> then nullCheck dbObj
        elif localType = typeof<option<bool>> then makeOption<bool> dbObj
        elif hasInterface localType "IEnumerable" then makeCollections localType dbObj // String includes IEnumerable
        else 
            localType
            |> sprintf "Unsupported property/value: %s. Type: %A" name
            |> invalidOp
    
    let deserialize (typ : Type) (entity : IEntity) = 
        typ.GetProperties()
        |> Array.map (fun pi ->
            match entity.Properties.TryGetValue pi.Name with
            | true, v -> fixTypes pi.Name pi.PropertyType v
            | _ ->
                if isOption pi.PropertyType then box None 
                else 
                    pi.Name
                    |> sprintf "Could not deserialize node - the required value was not found on the database node: %s"
                    |> invalidOp)
      
    // Look into FSharpValue.PreComputeRecordConstructor - is it faster?
    // https://codeblog.jonskeet.uk/2008/08/09/making-reflection-fly-and-exploring-delegates/
    let toRecord (typ : Type) (entity : IEntity) = FSharpValue.MakeRecord(typ, deserialize typ entity)
    
    // Support parameterless constuctors
    let toClass (typ : Type) (entity : IEntity) = 
        let obs = deserialize typ entity
        if Array.isEmpty obs 
        then Activator.CreateInstance(typ)
        else Activator.CreateInstance(typ, obs)