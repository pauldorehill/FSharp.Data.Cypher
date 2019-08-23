namespace FSharp.Data.Cypher

type Label = 
    | Label of string
    member this.Value = match this with | Label x -> x
    override this.ToString() = this.Value

type IFSEntity = interface end

type IFSNode =
    inherit IFSEntity
    abstract member Labels : Label list option

type IFSRelationship =
    inherit IFSEntity
    abstract member Label : Label option

// This is a hack to enforce correct Types on operators while allowing chaining
// due to operator precendence can't find a better way
type Ascii = 
    | Ascii
    interface IFSNode with member __.Labels = None
    interface IFSRelationship with member __.Label = None

module Label =
    
    let forRelationship (value : string) = Some (Label value)
    
    let forNode (values : string list) = values |> List.map Label |> Some

[<AutoOpen>]
module Ascii =
    
    let (--) (startNode : IFSNode) (endNode : IFSNode) = Ascii
    
    let (-->) (startNode : IFSNode) (endNode : IFSNode) = Ascii
    
    let (<--) (startNode : IFSNode) (endNode : IFSNode) = Ascii
    
    let (|->) (relationship : IFSRelationship) (node : IFSNode) = Ascii
    
    let (|-) (relationship : IFSRelationship) (node : IFSNode) = Ascii
    
    let (-|) (node : IFSNode) (relationship : IFSRelationship) = Ascii

    let (<-|) (node : IFSNode) (relationship : IFSRelationship) = Ascii