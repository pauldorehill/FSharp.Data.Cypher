namespace FSharp.Data.Cypher

open System
open System.Collections
open System.Reflection
open FSharp.Reflection
open Neo4j.Driver.V1

module Deserialization =
    
    let isOption (typ : Type) = typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<Option<_>>

    // Driver returns a System.Collections.Generic.List`1[System.Object]
    let checkCollection<'T> (rtnObj : obj) =
        if isNull rtnObj then Seq.empty
        elif rtnObj.GetType() = typeof<Generic.List<obj>> then
            rtnObj 
            :?> Generic.List<obj>
            |> Seq.cast<'T>
        else 
            rtnObj :?> 'T |> Seq.singleton 

    let makeSeq<'T> rtnObj = checkCollection<'T> rtnObj

    let makeArray<'T> rtnObj = checkCollection<'T> rtnObj |> Array.ofSeq

    let makeList<'T> rtnObj = checkCollection<'T> rtnObj |> List.ofSeq

    let makeSet<'T when 'T : comparison> rtnObj = checkCollection<'T> rtnObj |> Set.ofSeq

    let makeOption<'T> (obj : obj) = 
        if isNull obj then box None 
        else obj :?> 'T |> Some |> box

    let fixTypes (name : string) (localType : Type) (rtnObj : obj) =

        let nullCheck (obj : obj) = 
            if isNull obj then
                sprintf "A null object was returned for %s on type %A" name localType.Name
                |> ArgumentNullException
                |> raise
            else obj

        if localType = typeof<string> then nullCheck rtnObj
        elif localType = typeof<int64> then nullCheck rtnObj
        elif localType = typeof<int32> then nullCheck rtnObj |> Convert.ToInt32 |> box
        elif localType = typeof<float> then nullCheck rtnObj
        elif localType = typeof<bool> then nullCheck rtnObj
        
        elif localType = typeof<string option> then makeOption<string> rtnObj
        elif localType = typeof<int64 option> then makeOption<int64> rtnObj
        elif localType = typeof<int32 option> then rtnObj |> Convert.ToInt32 |> makeOption<int32>
        elif localType = typeof<float option> then makeOption<float> rtnObj
        elif localType = typeof<bool option> then makeOption<bool> rtnObj

        elif localType = typeof<string seq> then makeSeq<string> rtnObj |> box
        elif localType = typeof<int64 seq> then makeSeq<int64> rtnObj |> box
        elif localType = typeof<int32 seq> then makeSeq<int64> rtnObj |> Seq.map Convert.ToInt32 |> box
        elif localType = typeof<float seq> then makeSeq<float> rtnObj |> box
        elif localType = typeof<bool seq> then makeSeq<bool> rtnObj |> box
            
        elif localType = typeof<string []> then makeArray<string> rtnObj |> box
        elif localType = typeof<int64 []> then makeArray<int64> rtnObj |> box
        elif localType = typeof<int32 []> then makeArray<int64> rtnObj |> Array.map Convert.ToInt32 |> box
        elif localType = typeof<float []> then makeArray<float> rtnObj |> box
        elif localType = typeof<bool []> then makeArray<bool> rtnObj |> box
            
        elif localType = typeof<string list> then makeList<string> rtnObj |> box
        elif localType = typeof<int64 list> then makeList<int64> rtnObj |> box
        elif localType = typeof<int32 list> then makeList<int64> rtnObj |> List.map Convert.ToInt32 |> box
        elif localType = typeof<float list> then makeList<float> rtnObj |> box
        elif localType = typeof<bool list> then makeList<bool> rtnObj |> box
            
        elif localType = typeof<string Set> then makeSet<string> rtnObj |> box
        elif localType = typeof<int64 Set> then makeSet<int64> rtnObj |> box
        elif localType = typeof<int32 Set> then makeSet<int64> rtnObj |> Set.map Convert.ToInt32 |> box
        elif localType = typeof<float Set> then makeSet<float> rtnObj |> box
        elif localType = typeof<bool Set> then makeSet<bool> rtnObj |> box
        // Here for when RETURN () -> that becomes RETURN null
        elif localType = typeof<unit> then box ()

        else 
            localType
            |> sprintf "Unsupported property/value: %s. Type: %A" name
            |> invalidOp

    let createRecordOrClass (typ : Type) (obs : obj []) =
        if FSharpType.IsRecord typ then FSharpValue.MakeRecord(typ, obs)
        elif typ.IsClass then
            if Array.isEmpty obs || obs.Length = 1 then Activator.CreateInstance typ
            else invalidOp "Only parameterless classes are supported"
        else invalidOp "Type was not a class or a record."

    let createNullRecordOrClass (typ : Type) =
        if FSharpType.IsRecord typ then 
            FSharpType.GetRecordFields(typ).Length
            |> Array.zeroCreate
            |> fun obs -> FSharpValue.MakeRecord(typ, obs)
        elif typ.IsClass then 
            let ctrs = typ.GetConstructors()
            if ctrs.Length = 0 then 
                (typ.Name, typ.Name)
                ||> sprintf "Class of Type: %s is static. It needs a parameterless constructor. eg. type %s () ="
                |> invalidOp 

            elif ctrs.Length = 1 && (ctrs |> Array.head |> fun c -> c.GetParameters() |> Array.isEmpty) 
            then Activator.CreateInstance typ
            else
                typ.Name
                |> sprintf "Only parameterless classes are supported. Type: %s"
                |> invalidOp 
            
        else 
            typ.Name
            |> sprintf "Type %s was not a F# record or class"
            |> invalidOp

    let getProperties (typ : Type) =
        let bindingFlags = BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic
        if FSharpType.IsRecord typ then FSharpType.GetRecordFields typ
        else typ.GetProperties bindingFlags

    // TODO: Class serialization is tricky due to the order that the values have to be passed
    // into the constructor doesn't match the decleration order on the class
    // also if there is a const member this.V = "Value" that is impossible to filter out with BindingFlags
    // will need to revisit how handle classes - currently just parameterless for now to allow
    // for relationships with no properties
    // Look into FSharpValue.PreComputeRecordConstructor - is it faster?
    // https://codeblog.jonskeet.uk/2008/08/09/making-reflection-fly-and-exploring-delegates/
    // Or use of quotations as an equvialent
    let deserialize (typ : Type) (entity : IEntity) =
        let mutable typeInstance = None
        
        let makeTypeInstance () = 
            typeInstance <- Some (createNullRecordOrClass typ)

        let makeObj (pi : PropertyInfo) =
            match entity.Properties.TryGetValue pi.Name with
            | true, v -> fixTypes pi.Name pi.PropertyType v
            | _ ->
                if isOption pi.PropertyType then box None
                elif true then
                    if typeInstance.IsNone then makeTypeInstance ()
                    // Try and find members that take no parameters
                    pi.GetValue typeInstance.Value
                    
                else
                    sprintf "Could not deserialize to %s type from the graph. The required property was not found: %s" typ.Name pi.Name
                    |> invalidOp

        typ
        |> getProperties 
        |> Array.map makeObj
        |> fun xs -> typ, xs

module Serialization =  

    let makeOption<'T> (o : obj) = 
        match o :?> 'T option with
        | Some o -> Some (box o) 
        | None -> None

    let checkCollection<'T> (rtnObj : obj) =
        let xs = rtnObj :?> Generic.IEnumerable<'T>
        if Seq.isEmpty xs then None
        else
            xs
            |> Seq.map box
            |> Generic.List
            |> box
            |> Some

    // TODO : 
    // Add in options and seq
    // make a single type check for both serializer and de, passing in the functions
    // Option is used here to avoid null - the null is inserted when sending off the final query
    let fixTypes (o : obj) =
        if isNull o then None
        else
            let typ = o.GetType()
            if typ = typeof<string> then Some o
            elif typ = typeof<int64> then Some o
            elif typ = typeof<int32> then Some o //:?> int32 |> int64 |> box
            elif typ = typeof<float> then Some o
            elif typ = typeof<bool> then Some o

            elif typ = typeof<string option> then makeOption<string> o
            elif typ = typeof<int64 option> then makeOption<int64> o
            elif typ = typeof<int32 option> then makeOption<int32> o
            elif typ = typeof<float option> then makeOption<float> o
            elif typ = typeof<bool option> then makeOption<bool> o

            elif typ = typeof<string seq> then checkCollection<string> o
            elif typ = typeof<int64 seq> then checkCollection<int64> o
            elif typ = typeof<int32 seq> then checkCollection<int32> o
            elif typ = typeof<float seq> then checkCollection<float> o
            elif typ = typeof<bool seq> then checkCollection<bool> o
            
            elif typ = typeof<string []> then checkCollection<string> o
            elif typ = typeof<int64 []> then checkCollection<int64> o
            elif typ = typeof<int32 []> then checkCollection<int32> o
            elif typ = typeof<float []> then checkCollection<float> o
            elif typ = typeof<bool []> then checkCollection<bool> o
            
            elif typ = typeof<string list> then checkCollection<string> o
            elif typ = typeof<int64 list> then checkCollection<int64> o
            elif typ = typeof<int32 list> then checkCollection<int32> o
            elif typ = typeof<float list> then checkCollection<float> o
            elif typ = typeof<bool list> then checkCollection<bool> o
            
            elif typ = typeof<string Set> then checkCollection<string> o
            elif typ = typeof<int64 Set> then checkCollection<int64> o
            elif typ = typeof<int32 Set> then checkCollection<int32> o
            elif typ = typeof<float Set> then checkCollection<float> o
            elif typ = typeof<bool Set> then checkCollection<bool> o
        
            else
                typ
                |> sprintf "Unsupported property/value: %s. Type: %A" typ.Name
                |> invalidOp

    let serialize (o : obj) =
        let typ = o.GetType()
        typ
        |> Deserialization.getProperties
        |> Array.choose (fun pi -> 
            match fixTypes (pi.GetValue o) with
            | Some o -> Some (pi.Name , o)
            | None -> None)
        |> Map.ofArray
        |> Generic.Dictionary

    //let makeIEntity (fsNode : #IFSEntity) = 
    //    { new IEntity with 
    //        member _.Id = invalidOp "Fake Entity."
    //        member this.get_Item (key : string) : obj = invalidOp "Fake Entity."
    //        member _.Properties = invalidOp "Fake Entity." }
    
    let makeINode (fsNode : #IFSNode) = 
        { new INode with 
            member _.Labels = invalidOp "Fake Node - no labels"
        interface IEntity with
            member _.Id = invalidOp "Fake Node - no id."
            member this.get_Item (key : string) : obj = invalidOp "Fake Node"
            member _.Properties = invalidOp "Fake Node"
        interface IEquatable<INode> with
            member this.Equals(other : INode) = this = (other :> IEquatable<INode>) }
            
    let makeIRelationship (fsRel : #IFSRelationship) = 
        { new IRelationship with 
            member _.StartNodeId = invalidOp "Fake relationsip - no start node"
            member _.EndNodeId = invalidOp "Fake relationsip - no end node"
            member _.Type = invalidOp "Fake relationship - no type name"
        interface IEntity with
            member _.Id = invalidOp "Fake Node - no id."
            member this.get_Item (key : string) : obj = this.Properties.Item key
            member _.Properties = serialize fsRel :> Generic.IReadOnlyDictionary<string,obj>
        interface IEquatable<IRelationship> with
            member this.Equals(other : IRelationship) = this = (other :> IEquatable<IRelationship>) }