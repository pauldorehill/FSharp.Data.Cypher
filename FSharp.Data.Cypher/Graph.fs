namespace FSharp.Data.Cypher

type IFSEntity = interface end

/// Marker interface for a type that is a Node
type IFSNode = interface end

/// Marker interface for a type that is a Relationship
type IFSRelationship = interface end

[<NoComparison; NoEquality>]
type NodeLabel(label : string) = 
    member __.Value = label
    override this.ToString() = NodeLabel.Make this.Value
    static member Make (label : string) =
        if label.Contains(" ") then sprintf "`%s`" label else label
        |> sprintf ":%s"

[<NoComparison; NoEquality>]
type RelLabel(label : string) = 
    member __.Value = label
    override this.ToString() = NodeLabel.Make this.Value
    static member inline (/) (r1 : RelLabel, r2 : RelLabel) = r2

/// Match a Relationship
[<NoComparison; NoEquality>]
type Rel(relationship : IFSRelationship) =
    interface IFSEntity

    new (label : RelLabel) = Rel { new IFSRelationship }
    //new (pathHops : int list) = Rel { new IFSRelationship }

    /// Match a Relationship with the label and bind it to the variable name. The (||) operator can be used to specify multiple relationships
    new (relationship : IFSRelationship, label : RelLabel) = Rel relationship

    /// Match all Relationships on the path within the path hops length and bind them to the variable name
    //new (relationship : IFSRelationship, pathHops : int list) = Rel relationship

    //new (relationship : IFSRelationship, label : RelLabel, pathHops : int list) = Rel relationship


// May need to remove the equality down the line to allow return full paths?
/// Match a Node
[<NoComparison; NoEquality>]
type Node() =
    interface IFSEntity
    /// Match a Node and bind it to the variable name
    new (node : IFSNode) = Node()

    /// Match a Node with the label and bind it to the variable name
    new (node : IFSNode, label : NodeLabel) = Node()

    /// Match a Node with the labels and bind it to the variable name
    new (node : IFSNode, labels : NodeLabel list) = Node()

    /// Match a Node with properties with the label
    /// (n:Node { Name : 'Hello' }) can be written as Node(n, NodeLabel "Node", { n with Name = 'Hello' })
    new (node : IFSNode, nodeWithProperties : IFSNode) = Node()

    /// Match a Node with properties with the label
    /// (n:Node { Name : 'Hello' }) can be written as Node(n, NodeLabel "Node", { n with Name = 'Hello' })
    new (node : IFSNode, label : NodeLabel, nodeWithProperties : IFSNode) = Node()

    /// Match a Node with properties with the labels
    /// (n:Node { Name : 'Hello' }) can be written as Node(n, NodeLabel "Node", { n with Name = 'Hello' })
    new (node : IFSNode, label : NodeLabel list, nodeWithProperties : IFSNode) = Node()

    static member (--) (n1 : Node, n2 : Node) = n2
    static member (--) (n : Node, r : Rel) = r
    static member (--) (r : Rel, n : Node) = n
    static member (-->) (n1 : Node, n2 : Node) = n2
    static member (-->) (n : Node, r : Rel) = r
    static member (-->) (r : Rel, n : Node) = n
    static member (<--) (n1 : Node, n2 : Node) = n1
    static member (<--) (n : Node, r : Rel) = n
    static member (<--) (r : Rel, n : Node) = r

[<AutoOpen>]
module Ascii =

    let inline (--) (graphEntity : IFSEntity) (graphEntity2 : IFSEntity) = graphEntity2
    
    let inline (-->) (graphEntity : IFSEntity) (graphEntity2 : IFSEntity) = graphEntity2
    
    let inline (<--) (graphEntity : IFSEntity) (graphEntity2 : IFSEntity) = graphEntity
