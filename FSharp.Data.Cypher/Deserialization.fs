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

    // TODO: Fix nulls
    let fixTypes (typ : Type) (o : obj) =
        if typ = typeof<int32> 
        then 
            let i = o :?> int64
            if i <= int64 Int32.MaxValue then i |> int |> box
            else InvalidCastException "Can't convert int64 to int32. Value returned is greater than Int32.MaxValue" |> raise
        elif typ = typeof<string>
        then 
            let s = o :?> string
            if isNull s then box String.Empty else o
        else o
    
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