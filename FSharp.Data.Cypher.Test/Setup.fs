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

module ``Movie Graph As Records`` =
    
    type Movie =
        { title : string
          tagline : string option
          released : int }
        interface IFSNode with
            member __.Labels = Some [ NodeLabel "Movie" ]
    
    type Person =
        { born : int
          name : string }
        interface IFSNode with
            member __.Labels =
                [ "Person" ]
                |> List.map NodeLabel
                |> Some
       
    type ActedIn =
        { roles : string list }
        interface IFSRelationship with
            member __.Label = Some (RelationshipLabel "ACTED_IN")
    
    type Directed =
        { forceToRecord : string option }// it becomes a class otherwise
        interface IFSRelationship with
            member __.Label = Some (RelationshipLabel "DIRECTED")

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
            member __.Labels = Some [ NodeLabel "Movie" ]

    type Person(born : int, name : string) =
        member __.born = born 
        member __.name = name
        interface IFSNode with
            member __.Labels =
                [ "Person" ]
                |> List.map NodeLabel
                |> Some
       
    type ActedIn(roles : string list) =
        member __.roles = roles
        interface IFSRelationship with
            member __.Label = Some (RelationshipLabel "ACTED_IN")
    
    type Directed() =
        interface IFSRelationship with
            member __.Label = Some (RelationshipLabel "DIRECTED")

    type Graph =
        static member Movie : Query<Movie> = NA
        static member ActedIn : Query<ActedIn> = NA
        static member Directed : Query<Directed> = NA
        static member Person : Query<Person> = NA