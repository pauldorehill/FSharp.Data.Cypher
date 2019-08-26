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
    abstract member Label : Label

// This is a hack to enforce correct Types on operators while allowing chaining
// due to operator precendence can't find a better way
type Ascii = 
    | Ascii
    interface IFSNode with 
        member __.Labels = None
    interface IFSRelationship with 
        member __.Label = Label ""

module Label =
    
    let forNode (values : string list) = values |> List.map Label |> Some

    let make (l : Label) =
        if l.Value.Contains(" ") then sprintf "`%s`" l.Value else l.Value
        |> sprintf ":%s"

[<AutoOpen>]
module Ascii =
    
    let (--) (startNode : IFSNode) (endNode : IFSNode) = Ascii
    
    let (-->) (startNode : IFSNode) (endNode : IFSNode) = Ascii
    
    let (<--) (startNode : IFSNode) (endNode : IFSNode) = Ascii
    
    let (|->) (relationship : IFSRelationship) (node : IFSNode) = Ascii
    
    let (|-) (relationship : IFSRelationship) (node : IFSNode) = Ascii
    
    let (-|) (node : IFSNode) (relationship : IFSRelationship) = Ascii

    let (<-|) (node : IFSNode) (relationship : IFSRelationship) = Ascii