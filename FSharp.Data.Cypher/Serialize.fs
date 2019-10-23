namespace FSharp.Data.Cypher

open System
open System.Collections
open System.Reflection
open FSharp.Reflection
open FSharp.Quotations
open Neo4j.Driver

type Deserializer = Generic.IReadOnlyDictionary<string,obj> -> string * Type -> Expr

type Serializer = obj -> obj option

module TypeHelpers =

    let isOption (typ : Type) = typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<Option<_>>

    let private hasInterface (interfaceTyp : Type) (typ : Type) =
        typ.GetInterfaces()
        |> Array.exists (fun typ ->
            if typ.IsGenericType
            then typ.GetGenericTypeDefinition() = interfaceTyp
            else typ = interfaceTyp)

    let isIFSNode (typ : Type) = hasInterface typedefof<IFSNode<_>> typ

    let isIFSRel (typ : Type) = hasInterface typedefof<IFSRel<_>> typ

    let isIFS (typ : Type) = isIFSNode typ || isIFSRel typ

    let bindingFlags = BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic

    let internal (|Record|SingleCaseDU|PrmLessClassStruct|) (typ : Type) =

        let mutable uCI = None
        let mutable uCIFields = None
        let isSingleCase() =
            let u = FSharpType.GetUnionCases(typ, bindingFlags)
            let fields = u.[0].GetFields() // Handle named fields vs Item
            if u.Length = 1 && fields.Length <= 1 then
                uCI <- Some u.[0]
                uCIFields <- Some fields
                true
            else false

        if FSharpType.IsRecord(typ, bindingFlags) then
            let props = FSharpType.GetRecordFields typ
            let nullMaker () = FSharpValue.MakeRecord(typ, Array.zeroCreate props.Length, true)
            Record (props, nullMaker)

        elif FSharpType.IsUnion(typ, bindingFlags) && isSingleCase() then
            let props = uCIFields.Value
            let nullMaker () = FSharpValue.MakeUnion(uCI.Value, Array.zeroCreate props.Length, true)
            SingleCaseDU (props, nullMaker, uCI.Value)

        elif not(FSharpType.IsUnion(typ, bindingFlags)) && (typ.IsClass || typ.IsValueType) then
            let ctrs = typ.GetConstructors bindingFlags
            if ctrs.Length = 0 
            then invalidOp (sprintf "You must define a parameterless constructor for Class of type %s" typ.Name)
            else
                let classMaker =
                    ctrs
                    |> Array.tryPick (fun c -> if c.GetParameters().Length = 0 then Some (fun () -> c.Invoke([||])) else None)

                match classMaker with
                | Some classMaker ->
                    let props = typ.GetProperties()
                    PrmLessClassStruct (props, classMaker)
                | None -> invalidOp "Only Classes/Structs with a parameterless constructor are supported"

        else invalidOp "Only parameterless classes, Single Case FSharp DUs, and FSharp Records are supported"

    let createNullRecordOrClass typ =
        match typ with
        | Record (_, nullMaker) -> nullMaker()
        | SingleCaseDU (_, nullMaker, _)  -> nullMaker()
        | PrmLessClassStruct (props, nullMaker) -> nullMaker()

    let getProperties typ =
        match typ with
        | Record (props, _) -> props
        | SingleCaseDU (props, _, _)  -> props
        | PrmLessClassStruct (props, _) -> props

module Deserialization =

    open TypeHelpers

    // Driver returns a System.Collections.Generic.List`1[System.Object]
    let private checkCollection<'T> (rtnObj : obj) =
        if isNull rtnObj then Seq.empty
        elif rtnObj.GetType() = typeof<ResizeArray<obj>> then rtnObj :?> ResizeArray<obj> |> Seq.cast<'T>
        else rtnObj :?> 'T |> Seq.singleton

    let private makeSeq<'T> rtnObj = checkCollection<'T> rtnObj

    let private makeArray<'T> rtnObj = checkCollection<'T> rtnObj |> Array.ofSeq

    let private makeList<'T> rtnObj = checkCollection<'T> rtnObj |> List.ofSeq

    let private makeSet<'T when 'T : comparison> rtnObj = checkCollection<'T> rtnObj |> Set.ofSeq

    let private makeOption<'T> (obj : obj) =
        if isNull obj then box None
        else obj :?> 'T |> Some |> box

    let private coreTypes (rtnType : Type) (name : string) (rtnObj : obj) =

        let nullCheck (obj : obj) =
            if isNull obj then
                sprintf "A null object was returned for %s of type %A" name rtnType.Name
                |> ArgumentNullException
                |> raise
            else obj

        match rtnType with
        | v when v = typeof<string> -> nullCheck rtnObj
        | v when v = typeof<int64> -> nullCheck rtnObj
        | v when v = typeof<int32> -> nullCheck rtnObj |> Convert.ToInt32 |> box
        | v when v = typeof<float> -> nullCheck rtnObj
        | v when v = typeof<bool> -> nullCheck rtnObj
        | v when v = typeof<string option> -> makeOption<string> rtnObj
        | v when v = typeof<int64 option> -> makeOption<int64> rtnObj
        | v when v = typeof<int32 option> -> rtnObj |> Convert.ToInt32 |> makeOption<int32>
        | v when v = typeof<float option> -> makeOption<float> rtnObj
        | v when v = typeof<bool option> -> makeOption<bool> rtnObj
        | v when v = typeof<string seq> -> makeSeq<string> rtnObj |> box
        | v when v = typeof<int64 seq> -> makeSeq<int64> rtnObj |> box
        | v when v = typeof<int32 seq> -> makeSeq<int64> rtnObj |> Seq.map Convert.ToInt32 |> box
        | v when v = typeof<float seq> -> makeSeq<float> rtnObj |> box
        | v when v = typeof<bool seq> -> makeSeq<bool> rtnObj |> box
        | v when v = typeof<string []> -> makeArray<string> rtnObj |> box
        | v when v = typeof<int64 []> -> makeArray<int64> rtnObj |> box
        | v when v = typeof<int32 []> -> makeArray<int64> rtnObj |> Array.map Convert.ToInt32 |> box
        | v when v = typeof<float []> -> makeArray<float> rtnObj |> box
        | v when v = typeof<bool []> -> makeArray<bool> rtnObj |> box
        | v when v = typeof<string list> -> makeList<string> rtnObj |> box
        | v when v = typeof<int64 list> -> makeList<int64> rtnObj |> box
        | v when v = typeof<int32 list> -> makeList<int64> rtnObj |> List.map Convert.ToInt32 |> box
        | v when v = typeof<float list> -> makeList<float> rtnObj |> box
        | v when v = typeof<bool list> -> makeList<bool> rtnObj |> box
        | v when v = typeof<string Set> -> makeSet<string> rtnObj |> box
        | v when v = typeof<int64 Set> -> makeSet<int64> rtnObj |> box
        | v when v = typeof<int32 Set> -> makeSet<int64> rtnObj |> Set.map Convert.ToInt32 |> box
        | v when v = typeof<float Set> -> makeSet<float> rtnObj |> box
        | v when v = typeof<bool Set> -> makeSet<bool> rtnObj |> box
        | v when v = typeof<unit> -> box ()
        | _ ->  invalidOp(sprintf "Unsupported type of: %s" rtnType.Name)

    let private complexTypes (rtnType : Type) (iEntity : #IEntity) =

        let makeObj props nullMaker =
            let mutable typeInstance = None

            let makeTypeInstance () =
                typeInstance <- Some (nullMaker ())

            // This is here for classes/structs -> need to guard against DU
            let isParameterlessProp (pi : PropertyInfo) =
                not (FSharpType.IsUnion(rtnType, bindingFlags))
                && pi.GetMethod.GetParameters() |> Array.isEmpty

            let maker (pi : PropertyInfo) =
                match iEntity.Properties.TryGetValue pi.Name with
                | true, rtnObj -> coreTypes pi.PropertyType pi.Name rtnObj
                | false, _ when isOption pi.PropertyType -> box None
                | false, _ when isParameterlessProp pi ->
                    if typeInstance.IsNone then makeTypeInstance ()
                    pi.GetValue typeInstance.Value // null check here?
                | _ ->
                    sprintf "Could not deserialize to %s type from the graph. The required property was not found: %s" rtnType.Name pi.Name
                    |> invalidOp

            Array.map maker props

        if isNull iEntity then
            sprintf "A null IEntity (Node / Relationship) was returned from the database of expected type %s. This is likely from an OPTIONAL MATCH" rtnType.Name
            |> NullReferenceException
            |> raise
        else
            match rtnType with
            | Record (props, nullMaker) ->
                let obs = makeObj props nullMaker
                FSharpValue.MakeRecord(rtnType, obs, true)
            | SingleCaseDU (props, nullMaker, ucs) ->
                if props.Length = 0 then nullMaker()
                else
                    let obs = makeObj props nullMaker
                    FSharpValue.MakeUnion(ucs, obs, true)
            | PrmLessClassStruct (_, nullMaker) -> nullMaker() // No members to set

    // https://codeblog.jonskeet.uk/2008/08/09/making-reflection-fly-and-exploring-delegates/
    // Or use of quotations as an equvialent
    let deserialize (continuation : Generic.IReadOnlyDictionary<string,obj>) (key : string, rtnType : Type) =
        match rtnType with
        | rtnTyp when isIFSNode rtnTyp -> continuation.[key] :?> INode |> complexTypes rtnType
        | rtnTyp when rtnTyp.IsGenericType 
            && rtnTyp.GetGenericTypeDefinition() = typedefof<_ list>
            && isIFSNode rtnTyp.GenericTypeArguments.[0] ->
                continuation.[key] 
                :?> ResizeArray<obj>
                |> Seq.cast<INode>
                |> Seq.map (complexTypes rtnTyp.GenericTypeArguments.[0]) //TODO this needs to be the list type
                |> List.ofSeq
                |> box
        | rtnTyp when isIFSRel rtnTyp -> continuation.[key] :?> IRelationship |> complexTypes rtnType
        | _ -> coreTypes rtnType key continuation.[key]
        |> fun rtnObj -> Expr.Value(rtnObj, rtnType)

module Serialization =

    open TypeHelpers

    let private makeOption<'T> (o : obj) =
        match o :?> 'T option with
        | Some o -> Some (box o)
        | None -> None

    let private checkCollection<'T> (sndObj : obj) =
        let xs = sndObj :?> Generic.IEnumerable<'T>
        if Seq.isEmpty xs then None
        else
            xs
            |> Seq.map box
            |> ResizeArray
            |> box
            |> Some

    // TODO :
    // make a single type check for both serializer and de, passing in the functions
    // Option is used here to avoid null - the null is inserted when sending off the final query
    let private coreTypes (o : obj) =
        match o.GetType() with
        | sndType when sndType = typeof<string> -> Some o
        | sndType when sndType = typeof<int64> -> Some o
        | sndType when sndType = typeof<int32> -> Some o //:?> int32 |> int64 |> box
        | sndType when sndType = typeof<float> -> Some o
        | sndType when sndType = typeof<bool> -> Some o
        | sndType when sndType = typeof<string option> -> makeOption<string> o
        | sndType when sndType = typeof<int64 option> -> makeOption<int64> o
        | sndType when sndType = typeof<int32 option> -> makeOption<int32> o
        | sndType when sndType = typeof<float option> -> makeOption<float> o
        | sndType when sndType = typeof<bool option> -> makeOption<bool> o
        | sndType when sndType = typeof<string seq> -> checkCollection<string> o
        | sndType when sndType = typeof<int64 seq> -> checkCollection<int64> o
        | sndType when sndType = typeof<int32 seq> -> checkCollection<int32> o
        | sndType when sndType = typeof<float seq> -> checkCollection<float> o
        | sndType when sndType = typeof<bool seq> -> checkCollection<bool> o
        | sndType when sndType = typeof<string []> -> checkCollection<string> o
        | sndType when sndType = typeof<int64 []> -> checkCollection<int64> o
        | sndType when sndType = typeof<int32 []> -> checkCollection<int32> o
        | sndType when sndType = typeof<float []> -> checkCollection<float> o
        | sndType when sndType = typeof<bool []> -> checkCollection<bool> o
        | sndType when sndType = typeof<string list> -> checkCollection<string> o
        | sndType when sndType = typeof<int64 list> -> checkCollection<int64> o
        | sndType when sndType = typeof<int32 list> -> checkCollection<int32> o
        | sndType when sndType = typeof<float list> -> checkCollection<float> o
        | sndType when sndType = typeof<bool list> -> checkCollection<bool> o
        | sndType when sndType = typeof<string Set> -> checkCollection<string> o
        | sndType when sndType = typeof<int64 Set> -> checkCollection<int64> o
        | sndType when sndType = typeof<int32 Set> -> checkCollection<int32> o
        | sndType when sndType = typeof<float Set> -> checkCollection<float> o
        | sndType when sndType = typeof<bool Set> -> checkCollection<bool> o
        | sndType -> invalidOp (sprintf "Unsupported property/value: %s. Type: %A" sndType.Name sndType)

    let private complexTypes (o : obj) =
        o.GetType()
        |> getProperties
        |> Array.choose (fun pi ->
            match coreTypes (pi.GetValue o) with
            | Some o -> Some (pi.Name, o)
            | None -> None)
        |> function
        | [||] -> None
        | xs ->
            Map.ofArray xs
            |> Generic.Dictionary
            |> box
            |> Some

    let serialize (o : obj) =
        if isNull o then None
        else
            match o.GetType() with
            | v when isIFSNode v -> complexTypes o
            | v when isIFSRel v -> complexTypes o
            | _ -> coreTypes o

    let makeIEntity (id , di : Generic.IReadOnlyDictionary<string,obj>) =
        { new IEntity with
            member _.Id = id
            member this.get_Item (key : string) : obj = this.Properties.[key]
            member _.Properties = di }

    let makeINode (id, labels : NodeLabel list , di : Generic.IReadOnlyDictionary<string,obj>) =
        { new INode with
            member _.Labels =
                labels
                |> List.map (fun x -> x.Value)
                |> fun xs -> ResizeArray(xs).AsReadOnly()
                :> Generic.IReadOnlyList<string>
        interface IEntity with
            member _.Id = id
            member this.get_Item (key : string) : obj = this.Properties.[key]
            member _.Properties = di
        interface IEquatable<INode> with
            member this.Equals(other : INode) = this = (other :> IEquatable<INode>) }

    let makeIRelationship (startNodeId, endNodeId, label : RelLabel , di : Generic.IReadOnlyDictionary<string,obj>) =
        { new IRelationship with
            member _.StartNodeId = startNodeId
            member _.EndNodeId = endNodeId
            member _.Type = label.Value
        interface IEntity with
            member _.Id = invalidOp "Fake Node - no id."
            member this.get_Item (key : string) : obj = this.Properties.Item key
            member _.Properties = di
        interface IEquatable<IRelationship> with
            member this.Equals(other : IRelationship) = this = (other :> IEquatable<IRelationship>) }