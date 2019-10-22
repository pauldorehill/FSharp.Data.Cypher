namespace FSharp.Data.Cypher

open System
/// Marker interface for a type that is a Node
type IFSNode<'N> = interface end

/// Marker interface for a type that is a Relationship
type IFSRel<'R> = interface end

[<Sealed; NoComparison; NoEquality>]
type AS<'T>() =
    member _.AS (variable : AS<'T>) : 'T = invalidOp "AS.AS should never be called"

[<AbstractClass; Sealed>]
type AS =
    static member internal IsTypeDefOf (typ : Type) =
        typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<AS<_>>

[<AbstractClass; Sealed>]
type private Label =
    static member Make (label : string) =
        match label with
        | s when s = "" -> ""
        | s when s.Contains " " -> sprintf ":`%s`" label
        | _ -> sprintf ":%s" label

[<Sealed; NoComparison; NoEquality>]
type NodeLabel(label : string) =
    member _.Value = Label.Make label
    override this.ToString() = this.Value

// Neo technically calls these Type, however the use of type is a bit confusing so label has been used.
[<Sealed; NoComparison; NoEquality>]
type RelLabel(label : string) =
    member _.Value = Label.Make label
    static member ( + ) (r1 : RelLabel, r2 : RelLabel) =
        match r1.Value, r2.Value with
        | s1, s2 when s1 = "" && s2 = "" -> r1
        | s1, _ when s1 = "" -> r2
        | _, s2 when s2 = "" -> r1
        | s1, s2 -> s1.[1..] + "|" + s2 |> RelLabel

    static member (/) (r1 : RelLabel, r2 : RelLabel) = r1 + r2
    static member get_Zero() = RelLabel "" // TODO: here to allow use of sumBy: not sure if I should keep
    override this.ToString() = this.Value

/// Match any Relationship
[<Sealed; NoComparison; NoEquality>]
type Rel<'R>() =
    interface IFSRel<'R>
    /// Match a Relationship and bind it to the variable name
    new(relationship : IFSRel<'R>) = Rel<'R>()

    /// Match a Relationship with the label
    new(label : RelLabel) = Rel<'R>()

    /// Match a path length
    new(pathHops : uint32) = Rel<'R>()

    /// Match a variable path length. It will use the minimum & max value in the list.
    /// List expressions [ 0u .. 5u ] & list literals are accepted [ 0u; 5u ]
    new(pathHopsRange : uint32 list) = Rel<'R>()

    /// Match a Relationship with the label and bind it to the variable name.
    /// The (/) operator can be used to specify multiple relationships
    new(relationship : IFSRel<'R>, label : RelLabel) = Rel<'R>()

    /// Match a Relationship with the label and a path length
    new(label : RelLabel, pathHops : uint32) = Rel<'R>()

    /// Match a Relationship with the label and a variable path length.
    /// It will use the minimum & max value in the list.
    /// List expressions [ 0u .. 5u ] & list literals are accepted [ 0u; 5u ]
    new(label : RelLabel, pathHopsRange : uint32 list) = Rel<'R>()

    /// Match a Relationship with the label and properties and bind it to the variable name.
    /// The (/) operator can be used to specify multiple relationships
    new(relationship : IFSRel<'R>, label : RelLabel, relationshipWithProperties : IFSRel<'R>) = Rel<'R>()

[<AbstractClass; Sealed>]
type Rel =
    static member internal IsTypeDefOf (o : obj) =
        let typ = o.GetType()
        typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<Rel<_>>

// May need to remove the equality down the line to allow return full paths?
/// Match any Node
[<Sealed; NoComparison; NoEquality>]
type Node<'N>() =
    interface IFSNode<'N>
    /// Match a Node with the label, do not bind to any variable name
    new(label : NodeLabel) = Node<'N>()

    /// Match a Node with all the labels, do not bind to any variable name
    new(label : NodeLabel list) = Node<'N>()

    /// Match a Node and bind it to the variable name
    new(node : IFSNode<'N>) = Node<'N>()

    /// Match a Node with the label and bind it to the variable name
    new(node : IFSNode<'N>, label : NodeLabel) = Node<'N>()

    /// Match a Node with the labels and bind it to the variable name
    new(node : IFSNode<'N>, labels : NodeLabel list) = Node<'N>()

    /// Match a Node with properties with the label
    /// (n { Name : "Hello" }) can be written as Node(n, { n with Name = "Hello" })
    new(node : IFSNode<'N>, nodeWithProperties : IFSNode<'N>) = Node<'N>()

    /// Match a Node with properties with the label
    /// (n:Node { Name : "Hello" }) can be written as Node(n, NodeLabel "Node", { n with Name = "Hello" })
    new(node : IFSNode<'N>, label : NodeLabel, nodeWithProperties : IFSNode<'N>) = Node<'N>()

    /// Match a Node with properties with the labels
    /// (n:Node:Node2 { Name : "Hello" }) can be written as Node(n, [ NodeLabel "Node"; NodeLabel "Node2" ], { n with Name = "Hello" })
    new(node : IFSNode<'N>, label : NodeLabel list, nodeWithProperties : IFSNode<'N>) = Node<'N>()

    static member ( ---- ) (n1 : Node<'N>, n2 : Node<'N>) = n2
    static member ( ----> ) (node1 : Node<'N>, node2 : Node<'N>) = node2
    static member ( <---- ) (n1 : Node<'N>, n2 : Node<'N>) = n1
    static member ( -- ) (n : Node<'N>, r : Rel<'R>) = r
    static member ( -- ) (r : Rel<'R>, n : Node<'N>) = n
    static member ( --> ) (r : Rel<'R>, n : Node<'N>) = n
    static member ( <-- ) (n : Node<'N>, r : Rel<'R>) = r

[<AbstractClass; Sealed>]
type Node =
    static member internal IsTypeDefOf (o : obj) =
        let typ = o.GetType()
        typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<Node<_>>

// TODO: Paths... Path<'P> ...
[<AutoOpen>]
module Ascii =

    // Need this because:
    // Quotations cannot contain expressions that make member constraint calls,
    // or uses of operators that implicitly resolve to a member constraint call

    let inline ( -- ) graphEntity1 graphEntity2 = graphEntity1 -- graphEntity2

    let inline ( --> ) graphEntity1 graphEntity2 = graphEntity1 --> graphEntity2

    let inline ( <-- ) graphEntity1 graphEntity2 = graphEntity1 <-- graphEntity2

    let inline ( ---- ) node1 node2 = node1 ---- node2

    let inline ( ----> ) node1 node2 = node1 ----> node2

    let inline ( <---- ) node1 node2 = node1 <---- node2