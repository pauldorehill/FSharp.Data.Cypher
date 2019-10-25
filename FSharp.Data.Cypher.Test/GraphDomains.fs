namespace FSharp.Data.Cypher.Test.GraphDomains

open FSharp.Data.Cypher
open Neo4j.Driver

module MovieGraph =

    type Movie =
        { title : string
          tagline : string option
          released : int }
        member _.Label = NodeLabel "Movie"
        interface IFSNode<Movie> with
            member this.Labels = Some [ this.Label ]

    type Person =
        { born : int option
          name : string }
        member _.Label = NodeLabel "Person"
        interface IFSNode<Person> with
            member this.Labels = Some [ this.Label ]

    type ActedIn =
        { roles : string [] }
        member _.Label = RelLabel "ACTED_IN"
        interface IFSRel<ActedIn> with
            member this.Label = this.Label

    type Directed =
        private | NA
        member _.Label = RelLabel "DIRECTED"
        interface IFSRel<Directed> with
            member this.Label = this.Label

    type Follows =
        | NA
        member _.Label = RelLabel "FOLLOWS"
        interface IFSRel<Follows> with
            member this.Label = this.Label

    type Produced() =
        member _.Label = RelLabel "PRODUCED"
        interface IFSRel<Produced> with
            member this.Label = this.Label

    type Reviewed =
        { summary : string
          rating : int }
        member _.Label = RelLabel "REVIEWED"
        interface IFSRel<Reviewed> with
            member this.Label = this.Label

    type Wrote() =
        member _.Label = RelLabel "WROTE"
        interface IFSRel<Wrote> with
            member this.Label = this.Label

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