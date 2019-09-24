namespace FSharp.Data.Cypher.Test

open FSharp.Data.Cypher
open Neo4j.Driver.V1
open System.Collections
open Xunit
    
type AllAllowed =
    { string : string
      stringOption : string option 
      stringSeq : string seq 
      stringList : string List 
      stringArray : string array 
      stringSet : string Set 

      int64 : int64
      int64Option : int64 option 
      int64Seq : int64 seq 
      int64List : int64 List 
      int64Array : int64 array 
      int64Set : int64 Set 

      int32 : int32
      int32Option : int32 option 
      int32Seq : int32 seq 
      int32List : int32 List 
      int32Array : int32 array 
      int32Set : int32 Set 

      float : float
      floatOption : float option 
      floatSeq : float seq 
      floatList : float List 
      floatArray : float array 
      floatSet : float Set 

      bool : bool
      boolOption : bool option 
      boolSeq : bool seq 
      boolList : bool List 
      boolArray : bool array 
      boolSet : bool Set }
    interface IFSNode

module Graph =  
    
    // Need to have a Neo4j instance running with Auth disabled
    let uri = "bolt://localhost:7687"
    let driver = GraphDatabase.Driver(uri, AuthTokens.None)

module ``All Allowed`` =

    let makeToReturnType xs =
        xs
        |> Map.ofList
        |> Generic.Dictionary
        |> fun xs ->
            { new IEntity with 
                member _.Id = invalidOp "Fake Entity."
                member this.get_Item (key : string) : obj = this.Properties.Item key
                member _.Properties = xs :> Generic.IReadOnlyDictionary<string,obj> }

    let baseList =
        [ "string", box "EMU"
          "stringOption", box "EMU"
          "stringSeq", box ([ box "EMU" ] |> Generic.List)
          "stringList", box ([ box "EMU" ] |> Generic.List)
          "stringArray", box ([ box "EMU" ] |> Generic.List)
          "stringSet", box ([ box "EMU" ] |> Generic.List)
          "int64", box 5L
          "int64Option", box 5L
          "int64Seq", box ([ box 5L ] |> Generic.List)
          "int64List", box ([ box 5L ] |> Generic.List)
          "int64Array", box ([ box 5L ] |> Generic.List)
          "int64Set", box ([ box 5L ] |> Generic.List)
          "int32", box 5L
          "int32Option", box 5L
          "int32Seq", box ([ box 5L ] |> Generic.List)
          "int32List", box ([ box 5L ] |> Generic.List)
          "int32Array", box ([ box 5L ] |> Generic.List)
          "int32Set", box ([ box 5L ] |> Generic.List)
          "float", box 5.5
          "floatOption", box 5.5
          "floatSeq", box ([ box 5.5 ] |> Generic.List)
          "floatList", box ([ box 5.5 ] |> Generic.List)
          "floatArray", box ([ box 5.5 ] |> Generic.List)
          "floatSet", box ([ box 5.5 ] |> Generic.List)
          "bool", box true
          "boolOption", box true
          "boolSeq", box ([ box true ] |> Generic.List)
          "boolList", box ([ box true ] |> Generic.List)
          "boolArray", box ([ box true ] |> Generic.List)
          "boolSet", box ([ box true ] |> Generic.List) ]

    let allAllowedSome = makeToReturnType baseList
    
    let allAllowedNone = 
        baseList 
        |> List.filter (fun (n, _) -> not(n.ToLower().Contains "option"))
        |> makeToReturnType

    let nonCollections =
        [ "string", box "EMU"
          "stringOption", box "EMU"
          "stringSeq", box "EMU"
          "stringList", box "EMU"
          "stringArray", box "EMU"
          "stringSet", box "EMU"
          "int64", box 5L
          "int64Option", box 5L
          "int64Seq", box 5L
          "int64List", box 5L
          "int64Array", box 5L
          "int64Set", box 5L
          "int32", box 5L
          "int32Option", box 5L
          "int32Seq", box 5L
          "int32List", box 5L
          "int32Array", box 5L
          "int32Set", box 5L
          "float", box 5.5
          "floatOption", box 5.5
          "floatSeq", box 5.5
          "floatList", box 5.5
          "floatArray", box 5.5
          "floatSet", box 5.5
          "bool", box false
          "boolOption", box false
          "boolSeq", box false
          "boolList", box false
          "boolArray", box false
          "boolSet", box false ]
        |> makeToReturnType


module ``Movie Graph As Records`` =
    
    type LotsOfLabels() =
        interface IFSNode
        member _.Labels = [ "Label1"; "Label2"; "Label3"; "Label4"] |> List.map NodeLabel
    
    type LabelWithSpace() =
        interface IFSNode
        member _.Labels = NodeLabel "Label with spaces"

    type Movie =
        { title : string
          tagline : string option
          released : int }
        interface IFSNode
        member _.Label = NodeLabel "Movie"
    
    type Person =
        { born : int
          name : string }
        interface IFSNode
        member _.Labels = NodeLabel "Person"
      
    type ActedIn =
        { roles : string list }
        interface IFSRelationship
        member _.Label = RelLabel "ACTED_IN"
    
    type Directed() =
        interface IFSRelationship
        member _.Label = RelLabel "DIRECTED"
    
    type Graph =
        static member Movie : Query<Movie> = NA
        static member ActedIn : Query<ActedIn> = NA
        static member Directed : Query<Directed> = NA
        static member Person : Query<Person> = NA
        static member LotsOfLabels : Query<LotsOfLabels> = NA
        static member LabelWithSpace : Query<LabelWithSpace> = NA