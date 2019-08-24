namespace FSharp.Data.Cypher.Test

open FSharp.Data.Cypher
open Neo4j.Driver.V1
open System
open Xunit

[<AutoOpen>]
module Graph =  
    
    // Need to have a Neo4j instance running with Auth disabled
    let uri = "bolt://localhost:7687"
    let driver = GraphDatabase.Driver(uri, AuthTokens.None)

module ``Super Type`` =
    
    type AllAllowed =
        { string : string
          stringOption : string option 
          stringSeq : string seq 
          stringSeqOption : string seq option
          stringList : string List 
          stringListOption : string List option
          stringArray : string array 
          stringArrayOption : string array option
          stringSet : string Set 
          stringSetOption : string Set option

          int64 : int64
          int64Option : int64 option 
          int64Seq : int64 seq 
          int64SeqOption : int64 seq option
          int64List : int64 List 
          int64ListOption : int64 List option
          int64Array : int64 array 
          int64ArrayOption : int64 array option
          int64Set : int64 Set 
          int64SetOption : int64 Set option 

          int32 : int32
          int32Option : int32 option 
          int32Seq : int32 seq 
          int32SeqOption : int32 seq option
          int32List : int32 List 
          int32ListOption : int32 List option
          int32Array : int32 array 
          int32ArrayOption : int32 array option
          int32Set : int32 Set 
          int32SetOption : int32 Set option 

          float : float
          floatOption : float option 
          floatSeq : float seq 
          floatSeqOption : float seq option
          floatList : float List 
          floatListOption : float List option
          floatArray : float array 
          floatArrayOption : float array option
          floatSet : float Set 
          floatSetOption : float Set option 

          bool : bool
          boolOption : bool option 
          boolSeq : bool seq 
          boolSeqOption : bool seq option
          boolList : bool List 
          boolListOption : bool List option
          boolArray : bool array 
          boolArrayOption : bool array option
          boolSet : bool Set 
          boolSetOption : bool Set option }

module ``Movie Graph As Records`` =
    
    type Movie =
        { title : string
          tagline : string option
          released : int }
        interface IFSNode with
            member __.Labels = Some [ Label "Movie" ]
    
    type Person =
        { born : int
          name : string }
        interface IFSNode with
            member __.Labels =
                [ "Person" ]
                |> List.map Label
                |> Some
       
    type ActedIn =
        { roles : string list }
        interface IFSRelationship with
            member __.Label = Label "ACTED_IN"
    
    type Directed =
        { forceToRecord : string option }// it becomes a class otherwise
        interface IFSRelationship with
            member __.Label = Label "DIRECTED"

    type Graph =
        static member Movie : Query<Movie> = NA
        static member ActedIn : Query<ActedIn> = NA
        static member Directed : Query<Directed> = NA
        static member Person : Query<Person> = NA

module ``Movie Graph As Classes`` =

    type Movie(title : string, tagline : string option, released : int) =
        member __.title = title
        member __.tagline = tagline
        member __.released = released
        interface IFSNode with
            member __.Labels = Some [ Label "Movie" ]

    type Person(born : int, name : string) =
        member __.born = born 
        member __.name = name
        interface IFSNode with
            member __.Labels =
                [ "Person" ]
                |> List.map Label
                |> Some
       
    type ActedIn(roles : string list) =
        member __.roles = roles
        interface IFSRelationship with
            member __.Label = Label "ACTED_IN"
    
    type Directed() =
        interface IFSRelationship with
            member __.Label = Label "DIRECTED"

    type Graph =
        static member Movie : Query<Movie> = NA
        static member ActedIn : Query<ActedIn> = NA
        static member Directed : Query<Directed> = NA
        static member Person : Query<Person> = NA