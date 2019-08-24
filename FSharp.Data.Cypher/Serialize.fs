namespace FSharp.Data.Cypher

open System
open System.Collections
open System.Reflection
open FSharp.Reflection
open Neo4j.Driver.V1

module Deserialization =

    let isOption (typ : Type) = typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<Option<_>>

    let hasInterface (typ : Type) (name : string) = typ.GetInterface name |> isNull |> not
    
    // Driver returns a System.Collections.Generic.List`1[System.Object]
    let checkCollection<'T> (rtnObj : obj) = 
        if isNull rtnObj then Seq.empty
        elif rtnObj.GetType() = typeof<Generic.List<obj>> then
            rtnObj 
            :?> Generic.List<obj>
            |> Seq.cast<'T>
        else 
            rtnObj :?> 'T |> Seq.singleton 
    
    let checkCollectionOption<'T> (rtnObj : obj) = 
        if isNull rtnObj then None
        elif rtnObj.GetType() = typeof<Generic.List<obj>> then
            rtnObj 
            :?> Generic.List<obj>
            |> Seq.cast<'T>
            |> Some
        else 
            rtnObj :?> 'T |> Seq.singleton |> Some

    let makeSeq<'T> rtnObj = checkCollection<'T> rtnObj |> box

    let makeSeqOption<'T> rtnObj = checkCollectionOption<'T> rtnObj |> box

    let makeArray<'T> rtnObj = checkCollection<'T> rtnObj |> Array.ofSeq |> box

    let makeArrayOption<'T> rtnObj = checkCollectionOption<'T> rtnObj |> Option.map Array.ofSeq |> box

    let makeList<'T> rtnObj = checkCollection<'T> rtnObj |> List.ofSeq |> box

    let makeListOption<'T> rtnObj = checkCollectionOption<'T> rtnObj |> Option.map List.ofSeq |> box

    let makeSet<'T when 'T : comparison> rtnObj = checkCollection<'T> rtnObj |> Set.ofSeq |> box

    let makeSetOption<'T when 'T : comparison> rtnObj = checkCollectionOption<'T> rtnObj |> Option.map Set.ofSeq |> box

    let fixInt (obj : obj) =
        let i = obj :?> int64
        if i <= int64 Int32.MaxValue then i |> int |> box
        else InvalidCastException "Can't convert int64 to int32. Value returned is greater than Int32.MaxValue" |> raise

    let makeOption<'T> (obj : obj) = if isNull obj then box None else obj :?> 'T |> Some |> box

    let fixTypes (name : string) (localType : Type) (dbObj : obj) =

        let nullCheck (obj : obj) = 
            if isNull obj then
                localType.Name
                |> sprintf "A null object was returned for %s on type %A" name
                |> ArgumentNullException
                |> raise
            else obj

        let makeCollections(propTyp : Type) (rtnObj : obj) =
            if propTyp = typeof<string seq> then makeSeq<string> rtnObj
            elif propTyp = typeof<string seq option> then makeSeqOption<string> rtnObj
            elif propTyp = typeof<int64 seq> then makeSeq<int64> rtnObj
            elif propTyp = typeof<int64 seq option> then makeSeqOption<int64> rtnObj
            elif propTyp = typeof<int seq> then makeSeq<int> rtnObj
            elif propTyp = typeof<int seq option> then makeSeqOption<int> rtnObj
            elif propTyp = typeof<float seq> then makeSeq<float> rtnObj
            elif propTyp = typeof<float seq option> then makeSeqOption<float> rtnObj
            elif propTyp = typeof<bool seq option> then makeSeqOption<bool> rtnObj
            elif propTyp = typeof<bool seq> then makeSeq<bool> rtnObj
            
            elif propTyp = typeof<string array> then makeArray<string> rtnObj
            elif propTyp = typeof<string array option> then makeArrayOption<string> rtnObj
            elif propTyp = typeof<int64 array> then makeArray<int64> rtnObj
            elif propTyp = typeof<int64 array option> then makeArrayOption<int64> rtnObj
            elif propTyp = typeof<int array> then makeArray<int> rtnObj
            elif propTyp = typeof<int array option> then makeArrayOption<int> rtnObj
            elif propTyp = typeof<float array> then makeArray<float> rtnObj
            elif propTyp = typeof<float array option> then makeArrayOption<float> rtnObj
            elif propTyp = typeof<bool array option> then makeArrayOption<bool> rtnObj
            elif propTyp = typeof<bool array> then makeArray<bool> rtnObj
            
            elif propTyp = typeof<string list> then makeList<string> rtnObj
            elif propTyp = typeof<string list option> then makeListOption<string> rtnObj
            elif propTyp = typeof<int64 list> then makeList<int64> rtnObj
            elif propTyp = typeof<int64 list option> then makeListOption<int64> rtnObj
            elif propTyp = typeof<int list> then makeList<int> rtnObj
            elif propTyp = typeof<int list option> then makeListOption<int> rtnObj
            elif propTyp = typeof<float list> then makeList<float> rtnObj
            elif propTyp = typeof<float list option> then makeListOption<float> rtnObj
            elif propTyp = typeof<bool list option> then makeListOption<bool> rtnObj
            elif propTyp = typeof<bool list> then makeList<bool> rtnObj
            
            elif propTyp = typeof<string Set> then makeSet<string> rtnObj
            elif propTyp = typeof<string Set option> then makeSetOption<string> rtnObj
            elif propTyp = typeof<int64 Set> then makeSet<int64> rtnObj
            elif propTyp = typeof<int64 Set option> then makeSetOption<int64> rtnObj
            elif propTyp = typeof<int Set> then makeSet<int> rtnObj
            elif propTyp = typeof<int Set option> then makeSetOption<int> rtnObj
            elif propTyp = typeof<float Set> then makeSet<float> rtnObj
            elif propTyp = typeof<float Set option> then makeSetOption<float> rtnObj
            elif propTyp = typeof<bool Set option> then makeSetOption<bool> rtnObj
            elif propTyp = typeof<bool Set> then makeSet<bool> rtnObj

            else 
                propTyp.Name
                |> sprintf "Unsupported collection type: %s"
                |> invalidOp

        if localType = typeof<string> then nullCheck dbObj
        elif localType = typeof<string option> then makeOption<string> dbObj
        elif localType = typeof<int64> then nullCheck dbObj
        elif localType = typeof<int64 option> then makeOption<int64> dbObj
        elif localType = typeof<int32> then dbObj |> nullCheck |> fixInt
        elif localType = typeof<int32 option> then makeOption<int> dbObj
        elif localType = typeof<float> then nullCheck dbObj
        elif localType = typeof<float option> then makeOption<float> dbObj
        elif localType = typeof<bool> then nullCheck dbObj
        elif localType = typeof<bool option> then makeOption<bool> dbObj
        elif hasInterface localType "IEnumerable" then makeCollections localType dbObj // String includes IEnumerable
        else 
            localType
            |> sprintf "Unsupported property/value: %s. Type: %A" name
            |> invalidOp
    
    // TODO: Class serialization is tricky due to the order that the values have to be passed
    // into the constructure doesn't match the decleration order on the class
    // also if there is a const member this.V = "Value" that is impossible to filter out with BindingFlags
    // will need to revisit how handle classes - currently just parameterless for now to allow
    // for relationships with no properties

    // Look into FSharpValue.PreComputeRecordConstructor - is it faster?
    // https://codeblog.jonskeet.uk/2008/08/09/making-reflection-fly-and-exploring-delegates/
    
    let getProperties (typ : Type) =
        let bindingFlags = BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic
        if FSharpType.IsRecord typ then FSharpType.GetRecordFields typ
        else typ.GetProperties bindingFlags

    let deserialize (typ : Type) (entity : IEntity) =
        typ
        |> getProperties 
        |> Array.map (fun pi ->
            match entity.Properties.TryGetValue pi.Name with
            | true, v -> fixTypes pi.Name pi.PropertyType v
            | _ ->
                if isOption pi.PropertyType then box None 
                else 
                    pi.Name
                    |> sprintf "Could not deserialize IEntity from the graph. The required property was not found: %s"
                    |> invalidOp)

    let createRecordOrClass (typ : Type) (obs : obj []) =
        if FSharpType.IsRecord typ then FSharpValue.MakeRecord(typ, obs)
        elif typ.IsClass then
            if Array.isEmpty obs then Activator.CreateInstance typ
            else invalidOp "Only parameterless classes are supported"
        else invalidOp "Type was not a class or a record."

    let createNullRecordOrClass (typ : Type) =
        if FSharpType.IsRecord typ then 
            FSharpType.GetRecordFields(typ).Length
            |> Array.zeroCreate
            |> fun obs -> FSharpValue.MakeRecord(typ, obs)
        elif typ.IsClass then 
            let ctrs = typ.GetConstructors()
            if ctrs.Length = 1 && (ctrs |> Array.head |> fun c -> c.GetParameters() |> Array.isEmpty) 
            then Activator.CreateInstance typ
            else invalidOp "Only parameterless classes are supported"
            
        else invalidOp "Type was not a F# record."

module Serialization =  

    let fixTypes (o : obj) (pi : PropertyInfo) =
        let typ = pi.PropertyType
        if typ = typeof<int> then pi.GetValue o :?> int |> int64 |> box
        elif typ = typeof<string> then pi.GetValue o
        else
            typ
            |> sprintf "Unsupported property/value: %s. Type: %A" pi.Name
            |> invalidOp
        |> fun o -> pi.Name, o

    let serialize (e : #IFSEntity) =
        let typ = e.GetType()
        typ
        |> Deserialization.getProperties
        |> Array.map (fixTypes e)
        |> Map.ofArray
        |> Generic.Dictionary

    let makeIEntity (fsNode : #IFSEntity) = 
        { new IEntity with 
            member __.Id = invalidOp "Fake Entity."
            member this.get_Item (key : string) : obj = this.Properties.Item key
            member __.Properties = serialize fsNode :> Generic.IReadOnlyDictionary<string,obj> }
    
    let makeINode (fsNode : #IFSNode) = 
        { new INode with 
            member __.Labels = 
                fsNode.Labels
                |> Option.defaultValue []
                |> List.map string
                :> Generic.IReadOnlyList<string>
        interface IEntity with
            member __.Id = invalidOp "Fake Node - no id."
            member this.get_Item (key : string) : obj = this.Properties.Item key
            member __.Properties = serialize fsNode :> Generic.IReadOnlyDictionary<string,obj>
        interface IEquatable<INode> with
            member this.Equals(other : INode) = this = (other :> IEquatable<INode>) }
            
    let makeIRelationship (fsRel : #IFSRelationship) = 
        { new IRelationship with 
            member __.StartNodeId = invalidOp "Fake relationsip - no start node"
            member __.EndNodeId = invalidOp "Fake relationsip - no end node"
            member __.Type = fsRel.Label |> string
        interface IEntity with
            member __.Id = invalidOp "Fake Node - no id."
            member this.get_Item (key : string) : obj = this.Properties.Item key
            member __.Properties = serialize fsRel :> Generic.IReadOnlyDictionary<string,obj>
        interface IEquatable<IRelationship> with
            member this.Equals(other : IRelationship) = this = (other :> IEquatable<IRelationship>) }