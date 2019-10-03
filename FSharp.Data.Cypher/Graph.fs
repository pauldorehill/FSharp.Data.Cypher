namespace FSharp.Data.Cypher

type IFSEntity = interface end

/// Marker interface for a type that is a Node
type IFSNode = interface end

/// Marker interface for a type that is a Relationship
type IFSRelationship = interface end

[<Sealed>]
type private Label =
    static member Make (label : string) =
        match label with
        | s when s = "" -> ""
        | s when s.Contains " " -> sprintf ":`%s`" label
        | s -> sprintf ":%s" label

[<Sealed; NoComparison; NoEquality>]
type NodeLabel(label : string) = 
    member _.Value = Label.Make label
    override this.ToString() = this.Value

[<Sealed; NoComparison; NoEquality>]
type RelLabel(label : string) = 
    member _.Value = Label.Make label
    static member (+) (r1 : RelLabel, r2 : RelLabel) = 
        match r1.Value, r2.Value with
        | s1, s2 when s1 = "" && s2 = "" -> r1
        | s1, _ when s1 = "" -> r2
        | _, s2 when s2 = "" -> r1
        | s1, s2 -> s1.[1..] + "|" + s2 |> RelLabel

    static member (/) (r1 : RelLabel, r2 : RelLabel) = r1 + r2
    static member get_Zero() = RelLabel "" // here to allow use of sumBy: not sure if I should keep
    override this.ToString() = this.Value

/// Match any Relationship
[<Sealed; NoComparison; NoEquality>]
type Rel() =
    interface IFSEntity
    
    /// Match a Relationship and bind it to the variable name
    new (relationship : IFSRelationship) = Rel()

    /// Match a Relationship with the label
    new (label : RelLabel) = Rel()

    /// Match a path length 
    new (pathHops : uint32) = Rel()
    
    /// Match a variable path length. It will use the minimum & max value in the list. 
    /// List expressions [ 0u .. 5u ] & list literals are accepted [ 0u; 5u ]
    new (pathHopsRange : uint32 list) = Rel()

    /// Match a Relationship with the label and bind it to the variable name. 
    /// The (/) operator can be used to specify multiple relationships
    new (relationship : IFSRelationship, label : RelLabel) = Rel()
    
    /// Match a Relationship with the label and a path length 
    new (label : RelLabel, pathHops : uint32) = Rel()

    /// Match a Relationship with the label and a variable path length.
    /// It will use the minimum & max value in the list. 
    /// List expressions [ 0u .. 5u ] & list literals are accepted [ 0u; 5u ]
    new (label : RelLabel, pathHopsRange : uint32 list) = Rel()

    /// Match a Relationship with the label and properties and bind it to the variable name. 
    /// The (/) operator can be used to specify multiple relationships
    new (relationship : IFSRelationship, label : RelLabel, relationshipWithProperties : IFSRelationship) = Rel()

// May need to remove the equality down the line to allow return full paths?
/// Match any Node
[<Sealed; NoComparison; NoEquality>]
type Node() =
    interface IFSEntity
    /// Match a Node with the label, do not bind to any variable name
    new (label : NodeLabel) = Node()

    /// Match a Node with all the labels, do not bind to any variable name
    new (label : NodeLabel list) = Node()

    /// Match a Node and bind it to the variable name
    new (node : IFSNode) = Node()

    /// Match a Node with the label and bind it to the variable name
    new (node : IFSNode, label : NodeLabel) = Node()

    /// Match a Node with the labels and bind it to the variable name
    new (node : IFSNode, labels : NodeLabel list) = Node()

    /// Match a Node with properties with the label
    /// (n { Name : "Hello" }) can be written as Node(n, { n with Name = "Hello" })
    new (node : IFSNode, nodeWithProperties : IFSNode) = Node()

    /// Match a Node with properties with the label
    /// (n:Node { Name : "Hello" }) can be written as Node(n, NodeLabel "Node", { n with Name = "Hello" })
    new (node : IFSNode, label : NodeLabel, nodeWithProperties : IFSNode) = Node()

    /// Match a Node with properties with the labels
    /// (n:Node:Node2 { Name : "Hello" }) can be written as Node(n, [ NodeLabel "Node"; NodeLabel "Node2" ], { n with Name = "Hello" })
    new (node : IFSNode, label : NodeLabel list, nodeWithProperties : IFSNode) = Node()

    static member ( -- ) (n1 : Node, n2 : Node) = n2
    static member ( -- ) (n : Node, r : Rel) = r
    static member ( -- ) (r : Rel, n : Node) = n
    static member ( --> ) (node1 : Node, node2 : Node) = node2
    static member ( --> ) (n : Node, r : Rel) = r
    static member ( --> ) (r : Rel, n : Node) = n
    static member ( <-- ) (n1 : Node, n2 : Node) = n1
    static member ( <-- ) (n : Node, r : Rel) = n
    static member ( <-- ) (r : Rel, n : Node) = r

[<AutoOpen>]
module Ascii =
    // Need this because:
    // Quotations cannot contain expressions that make member constraint calls, 
    // or uses of operators that implicitly resolve to a member constraint call

    let inline ( -- ) (graphEntity : IFSEntity) (graphEntity2 : IFSEntity) = graphEntity2
    
    let inline ( --> ) (graphEntity : IFSEntity) (graphEntity2 : IFSEntity) = graphEntity2
    
    let inline ( <-- ) (graphEntity : IFSEntity) (graphEntity2 : IFSEntity) = graphEntity