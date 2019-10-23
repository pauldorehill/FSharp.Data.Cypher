namespace FSharp.Data.Cypher.Test.GraphDomains

open FSharp.Data.Cypher
open Neo4j.Driver

module MovieGraph =

    type Movie =
        { title : string
          tagline : string option
          released : int }
        member _.Label = NodeLabel "Movie"
        interface IFSNode<Movie>

    type Person =
        { born : int option
          name : string }
        member _.Label = NodeLabel "Person"
        interface IFSNode<Person>

    type ActedIn =
        { roles : string [] }
        member _.Label = RelLabel "ACTED_IN"
        interface IFSRel<ActedIn>

    type Directed =
        private | NA
        interface IFSRel<Directed>
        member _.Label = RelLabel "DIRECTED"

    type Follows =
        | NA
        interface IFSRel<Follows>
        member _.Label = RelLabel "FOLLOWS"

    type Produced() =
        interface IFSRel<Produced>
        member _.Label = RelLabel "PRODUCED"

    type Reviewed =
        { summary : string
          rating : int }
        interface IFSRel<Reviewed>
        member _.Label = RelLabel "REVIEWED"

    type Wrote() =
        interface IFSRel<Wrote>
        member _.Label = RelLabel "WROTE"

    type Graph =
        static member Movie = Node<Movie>()
        static member Person = Node<Person>()
        static member ActedIn = Rel<ActedIn>()
        static member Directed = Rel<Directed>()
        static member Follows = Rel<Follows>()
        static member Produced = Rel<Produced>()
        static member Reviewed = Rel<Reviewed>()
        static member Wrote = Rel<Wrote>()
         // Need to have a Neo4j instance running with Auth disabled
        static member Driver = GraphDatabase.Driver("bolt://localhost:7687", AuthTokens.None)