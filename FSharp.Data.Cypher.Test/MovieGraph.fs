namespace FSharp.Data.Cypher.Test

open System.Collections
open FSharp.Data.Cypher
open FSharp.Data.Cypher.Test
open Xunit

module MovieGraph =
    
    let driver = LocalGraph.Driver
    
    type Movie =
        { title : string
          tagline : string option
          released : int }
        interface IFSNode
        member _.Label = NodeLabel "Movie"
    
    type Person =
        { born : int option
          name : string }
        interface IFSNode
        member _.Label = NodeLabel "Person"
    
    type ActedIn =
        { roles : string [] }
        interface IFSRelationship
        member _.Label = RelLabel "ACTED_IN"
    
    type Directed() = 
        interface IFSRelationship
        member _.Label = RelLabel "DIRECTED"
    
    type Follows() = 
        interface IFSRelationship
        member _.Label = RelLabel "FOLLOWS"
    
    type Produced() = 
        interface IFSRelationship
        member _.Label = RelLabel "PRODUCED"
    
    type Reviewed = 
        { summary : string
          rating : int }
        interface IFSRelationship
        member _.Label = RelLabel "REVIEWED"
    
    type Wrote() = 
        interface IFSRelationship
        member _.Label = RelLabel "WROTE"
    
    type Graph =
        static member Movie : Query<Movie> = NA
        static member Person : Query<Person> = NA
        static member ActedIn : Query<ActedIn> = NA
        static member Directed : Query<Directed> = NA
        static member Follows : Query<Follows> = NA
        static member Produced : Query<Produced> = NA
        static member Reviewed : Query<Reviewed> = NA
        static member Wrote : Query<Wrote> = NA

    let movies () =
        cypher {
            for m in Graph.Movie do
            MATCH (Node(m, m.Label))
            RETURN m
        }
        |> Cypher.run driver
        |> QueryResult.results
    
    let people () =
        cypher {
            for p in Graph.Person do
            MATCH (Node(p, p.Label))
            RETURN p
        }
        |> Cypher.run driver
        |> QueryResult.results
    
    let actedIn () =
        cypher {
            for a in Graph.ActedIn do
            MATCH (Node() -- Rel(a, a.Label) -- Node())
            RETURN a
        }
        |> Cypher.run driver
        |> QueryResult.results
    
    let directed() =
        cypher {
            for d in Graph.Directed do
            MATCH (Node() -- Rel(d, d.Label) -- Node())
            RETURN d
        }
        |> Cypher.run driver
        |> QueryResult.results
    
    let follows () =
        cypher {
            for f in Graph.Follows do
            MATCH (Node() -- Rel(f, f.Label) -- Node())
            RETURN f
        }
        |> Cypher.run driver
        |> QueryResult.results
    
    let produced () =
        cypher {
            for pr in Graph.Produced do
        
            MATCH (Node() -- Rel(pr, pr.Label) -- Node())
            RETURN pr
        }
        |> Cypher.run driver
        |> QueryResult.results
    
    let reviewed () =
        cypher {
            for r in Graph.Reviewed do
            MATCH (Node() -- Rel(r, r.Label) -- Node())
            RETURN r
        }
        |> Cypher.run driver
        |> QueryResult.results
    
    let wrote () =
        cypher {
            for w in Graph.Wrote do
        
            MATCH (Node() -- Rel(w, w.Label) -- Node())
            RETURN w
        }
        |> Cypher.run driver
        |> QueryResult.results