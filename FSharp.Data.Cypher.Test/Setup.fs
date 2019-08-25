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