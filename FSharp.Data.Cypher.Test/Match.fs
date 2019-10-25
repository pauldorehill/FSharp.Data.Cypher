namespace FSharp.Data.Cypher.Test.MATCH

open System
open FSharp.Data.Cypher
open Xunit

// TODO: Remove all the 2 / 3 param cntr tests other than a single test:
// this is because there is full coverage of the indivual components in the single param ctrs
// so simple need to test the final output works

module Node =

    let node = { new IFSNode<'N> with member _.Labels = None }

    let label = "NodeLabel"

    let nodeLabel = NodeLabel label

    let nodeLabels = [ NodeLabel label; NodeLabel label; NodeLabel label ]

    type NodeType =
        { StringValue : string
          IntValue : int
          FloatValue : float }
        member _.Label = NodeLabel label
        interface IFSNode<NodeType> with
            member this.Labels = Some [ this.Label ]
        static member StaticLabel = NodeLabel label

    type Graph =
        static member Node = Node<IFSNode<_>>()
        static member NodeOfType = Node<NodeType>()

    module ``Empty Constructor`` =

        let rtnSt = sprintf "MATCH %s"

        [<Fact>]
        let ``Single Node`` () =

            cypher {
                MATCH (Node())
            }
            |> Cypher.query
            |> Query.raw
            |> fun q -> Assert.Equal(rtnSt "()", q)

        [<Fact>]
        let ``Two Nodes --`` () =

            cypher {
                MATCH (Node() ---- Node())
            }
            |> Cypher.query
            |> Query.raw
            |> fun q -> Assert.Equal(rtnSt "()--()", q)

        [<Fact>]
        let ``Two Nodes -->`` () =

            cypher {
                MATCH (Node() ----> Node())
            }
            |> Cypher.query
            |> Query.raw
            |> fun q -> Assert.Equal(rtnSt "()-->()", q)

        [<Fact>]
        let ``Two Nodes <--`` () =

            cypher {
                MATCH (Node() <---- Node())
            }
            |> Cypher.query
            |> Query.raw
            |> fun q -> Assert.Equal(rtnSt "()<--()", q)

        [<Fact>]
        let ``Complex sequence of Nodes`` () =

            cypher {
                MATCH (Node() <---- Node() ---- Node() ----> Node() <---- Node())
            }
            |> Cypher.query
            |> Query.raw
            |> fun q -> Assert.Equal(rtnSt "()<--()--()-->()<--()", q)

    module ``Single Parameter Constructor`` =

        module ``NodeLabel`` =

            let rtnSt = sprintf "MATCH (:%s)" label

            [<Fact>]
            let ``Create in Node Constructor`` () =
                cypher {
                    MATCH (Node(NodeLabel label))
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable outside function`` () =

                cypher {
                    MATCH (Node nodeLabel)
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable passed as function parameter`` () =
                let f (nodeLabel : NodeLabel) =
                    cypher {
                        MATCH (Node nodeLabel)
                    }

                f nodeLabel
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable in statement`` () =
                cypher {
                    let nodeLabel = NodeLabel label
                    MATCH (Node nodeLabel)
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

        module ``NodeLabel list`` =

            let nodeLabels = [ NodeLabel label; NodeLabel label; NodeLabel label]
            let rtnSt = sprintf "MATCH (:%s:%s:%s)" label label label

            [<Fact>]
            let ``Create in Node Constructor`` () =
                cypher {
                    MATCH (Node([ NodeLabel label; NodeLabel label; NodeLabel label]))
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable outside function`` () =

                cypher {
                    MATCH (Node nodeLabels)
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable passed as function parameter`` () =
                let f (nodeLabels : NodeLabel list) =
                    cypher {
                        MATCH (Node nodeLabels)
                    }

                f nodeLabels
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable in statement`` () =
                cypher {
                    let nodeLabels = [ NodeLabel label; NodeLabel label; NodeLabel label]
                    MATCH (Node nodeLabels)
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

        module ``IFSNode<'N>`` =

            let rtnSt = "MATCH (node)"
            let rtnStRaw = """MATCH ({StringValue: "NewStringValue", IntValue: 3, FloatValue: 2.1})"""
            let rtnStParams = """MATCH ({StringValue: $p00, IntValue: $p01, FloatValue: $p02})"""

            // Quotations can't contain object expressions
            // so use a graph for binding test
            [<Fact>]
            let ``Variable outside function`` () =
                cypher {
                    MATCH (Node node)
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable passed as function parameter`` () =
                let f (node : IFSNode<'N>) =
                    cypher {
                        MATCH (Node node)
                    }

                f node
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``For in do statement`` () =
                cypher {
                    for node in Graph.Node do
                    MATCH (Node node)
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Record update syntax for param matching`` () =
                cypher {
                    for node in Graph.NodeOfType do
                    MATCH (Node({ node with IntValue = 3; FloatValue = 2.1; StringValue = "NewStringValue" }))
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnStRaw, q)

            [<Fact>]
            let ``New Record: Raw Query`` () =
                cypher {
                    MATCH (Node({ IntValue = 3; FloatValue = 2.1; StringValue = "NewStringValue" }))
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnStRaw, q)

            [<Fact>]
            let ``New Record: Parameterized Query`` () =
                cypher {
                    MATCH (Node({ IntValue = 3; FloatValue = 2.1; StringValue = "NewStringValue" }))
                }
                |> Cypher.query
                |> Query.parameterized
                |> fun q -> Assert.Equal(rtnStParams, q)

            [<Fact>]
            let ``New Record: single field set`` () =
                cypher {
                    for node in Graph.NodeOfType do
                    MATCH (Node({ node with FloatValue = 2.1 }))
                }
                |> Cypher.query
                |> Query.parameterized
                |> fun q -> Assert.Equal("""MATCH ({FloatValue: $p00})""", q)

    module ``Two Parameter Constructor`` =

        module ``(IFSNode<'N>, NodeLabel)`` =

            let rtnSt = "MATCH (node:NodeLabel)"

            [<Fact>]
            let ``Variable outside function`` () =

                cypher {
                    MATCH (Node(node, nodeLabel))
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable passed as function parameter`` () =
                let f (node : IFSNode<'N>) (nodeLabel : NodeLabel) =
                    cypher {
                        MATCH (Node(node, nodeLabel))
                    }

                f node nodeLabel
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ```For in do statement`` () =
                cypher {
                    for node in Graph.NodeOfType do
                    MATCH (Node(node, node.Label))
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Let bound record call to label member: params in order`` () =
                cypher {
                    let node = { StringValue = "NewStringValue"; IntValue = 3; FloatValue = 2.1 }
                    MATCH (Node(node, node.Label))
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Let bound record call to label member: params reversed`` () =
                cypher {
                    let node = { IntValue = 3; FloatValue = 2.1; StringValue = "NewStringValue" }
                    MATCH (Node(node, node.Label))
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

        module ``(IFSNode<'N>, NodeLabel List)`` =

            let nodeLabels = [ NodeLabel label; NodeLabel label; NodeLabel label]

            let rtnSt = sprintf "MATCH (node:%s:%s:%s)" label label label

            [<Fact>]
            let ``Create in Node Constructor`` () =
                cypher {
                    MATCH (Node(node, [ NodeLabel label; NodeLabel label; NodeLabel label]))
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Node labels with List.map`` () =
                cypher {
                    MATCH (Node(node, List.map NodeLabel [ label; label; label ]))
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Let binding and Node labels with List.map`` () =
                cypher {
                    let labels = List.map NodeLabel [ label; label; label ]
                    MATCH (Node(node, labels))
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable outside function`` () =

                cypher {
                    MATCH (Node(node, nodeLabels))
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable passed as function parameter`` () =
                let f (node : IFSNode<'N>) (nodeLabels : NodeLabel list) =
                    cypher {
                        MATCH (Node(node, nodeLabels))
                    }

                f node nodeLabels
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable in statement`` () =
                cypher {
                    let nodeLabels = [ NodeLabel label; NodeLabel label; NodeLabel label]
                    MATCH (Node(node, nodeLabels))
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

        module ``(IFSNode<'N>, IFSNode<'N>)`` =

            let rtnStSingleProp = """MATCH (node {StringValue: "NewStringValue", IntValue: 3, FloatValue: 2.1})"""

            [<Fact>]
            let ``Record update syntax for param matching`` () =
                cypher {
                    for node in Graph.NodeOfType do
                    MATCH (Node(node, { node with IntValue = 3; FloatValue = 2.1; StringValue = "NewStringValue" }))
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnStSingleProp, q)

    module ``Three Parameter Constructor`` =

        module ``(IFSNode<'N>, NodeLabel, IFSNode<'N>)`` =

            let rtnStSingleProp = """MATCH (node:NodeLabel {StringValue: "NewStringValue", IntValue: 3, FloatValue: 2.1})"""

            [<Fact>]
            let ``Can make`` () =
                cypher {
                    for node in Graph.NodeOfType do
                    MATCH (Node(node, nodeLabel ,{ node with IntValue = 3; FloatValue = 2.1; StringValue = "NewStringValue" }))
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnStSingleProp, q)

        module ``(IFSNode<'N>, NodeLabel List, IFSNode<'N>)`` =

            let rtnStSingleProp = """MATCH (node:NodeLabel:NodeLabel:NodeLabel {StringValue: "NewStringValue", IntValue: 3, FloatValue: 2.1})"""

            [<Fact>]
            let ``Can make`` () =
                cypher {
                    for node in Graph.NodeOfType do
                    MATCH (Node(node, nodeLabels ,{ node with IntValue = 3; FloatValue = 2.1; StringValue = "NewStringValue" }))
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnStSingleProp, q)

module Relationship =

    let label = "REL_LABEL"
    let relLabel = RelLabel label
    let rel = { new IFSRel<'R> with member _.Label = relLabel}

    type RelType =
        { Value : string }
        member _.Label = RelLabel label
        interface IFSRel<RelType>
            with member this.Label = this.Label
        static member StaticLabel = RelLabel label
        member _.IntLabel = 3u
        static member IntLabelList = [ 0u .. 3u ]

    type Graph =
        static member Rel = Rel<IFSRel<_>>()
        static member RelOfType = Rel<RelType>()

    module ``Empty Constructor`` =

        [<Fact>]
        let ``Blank relationship`` () =
            cypher {
                MATCH (Node() -- Rel() -- Node())
            }
            |> Cypher.query
            |> Query.raw
            |> fun q -> Assert.Equal("MATCH ()-[]-()", q)

    module ``Single Parameter Constructor`` =

        module ``IFSRelationship<'R>`` =

            let rtnSt = "MATCH ()-[rel]-()"
            // Quotations can't contain object expressions
            // so use a graph for binding test

            [<Fact>]
            let ``Variable outside function`` () =
                cypher {
                    MATCH (Node() -- Rel rel -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable passed as function parameter`` () =
                let f (rel : IFSRel<'R>) =
                    cypher {
                        MATCH (Node() -- Rel rel -- Node())
                    }

                f rel
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``For in do statement`` () =
                cypher {
                    for rel in Graph.Rel do
                    MATCH (Node() -- Rel rel -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Record update syntax for param matching`` () =
                cypher {
                    for rel in Graph.RelOfType do
                    MATCH (Node() -- Rel({ rel with Value = "NewValue" }) -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal("""MATCH ()-[{Value: "NewValue"}]-()""", q)

        module ``RelLabel`` =

            let rtnSt = sprintf "MATCH ()-[:%s]-()" label

            [<Fact>]
            let ``Create in Rel Constructor`` () =
                cypher {
                    MATCH (Node() -- Rel(RelLabel label) -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable outside function`` () =

                cypher {
                    MATCH (Node() -- Rel relLabel -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable passed as function parameter`` () =
                let f (relLabel : RelLabel) =
                    cypher {
                        MATCH (Node() -- Rel relLabel -- Node())
                    }

                f relLabel
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable in statement`` () =
                cypher {
                    let relLabel = RelLabel label
                    MATCH (Node() -- Rel relLabel -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``For .. in with member`` () =
                cypher {
                    for rel in Graph.RelOfType do
                    MATCH (Node() -- Rel rel.Label -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``For .. in with Static member`` () =
                cypher {
                    MATCH (Node() -- Rel RelType.StaticLabel -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

        module ``Path Hops`` =

            [<Fact>]
            let ``Fixed no of path hops`` () =
                cypher {
                    MATCH (Node() -- Rel(3u) -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal("MATCH ()-[*3]-()", q)

            [<Fact>]
            let ``Fixed no of path hops on static member`` () =
                cypher {
                    MATCH (Node() -- Rel RelType.IntLabelList -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal("MATCH ()-[*0..3]-()", q)

            [<Fact>]
            let ``Fixed no of path hops on for in`` () =
                cypher {
                    for rel in Graph.RelOfType do
                    MATCH (Node() -- Rel rel.IntLabel -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal("MATCH ()-[*3]-()", q)

            [<Fact>]
            let ``Range of path hops`` () =
                cypher {
                    MATCH (Node() -- Rel([ 1u .. 3u ]) -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal("MATCH ()-[*1..3]-()", q)

            [<Fact>]
            let ``Range of path hops: Max path hops`` () =
                cypher {
                    MATCH (Node() -- Rel([ 0u .. UInt32.MaxValue ]) -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal("MATCH ()-[*0..]-()", q)

            [<Fact>]
            let ``List literal`` () =
                cypher {
                    MATCH (Node() -- Rel([ 1u; 0u; 3u; 5u; 3u; 7u; ]) -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal("MATCH ()-[*0..7]-()", q)

    module ``Two Parameter Constructor`` =

        module ``(IFSRelationship<'R>, RelLabel)`` =

            let rtnSt = "MATCH ()-[rel:REL_LABEL]-()"

            [<Fact>]
            let ``Variable outside function`` () =

                cypher {
                    MATCH (Node() -- Rel(rel, relLabel) -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable passed as function parameter`` () =
                let f (rel : IFSRel<'R>) (relLabel : RelLabel) =
                    cypher {
                        MATCH (Node() -- Rel(rel, relLabel) -- Node())
                    }

                f rel relLabel
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``For .. in with member`` () =
                cypher {
                    for rel in Graph.RelOfType do
                    MATCH (Node() -- Rel(rel, rel.Label) -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

        module ``(RelLabel, uint32)`` =

            let rtnSt = "MATCH ()-[:REL_LABEL*3]-()"
            let i = 3u

            [<Fact>]
            let ``Variable outside function`` () =

                cypher {
                    MATCH (Node() -- Rel(relLabel, 3u) -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable inside builder`` () =

                cypher {
                    let hops = 3u
                    MATCH (Node() -- Rel(relLabel, hops) -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variables passed as function parameter`` () =
                let f (relLabel : RelLabel) (hops : uint32) =
                    cypher {
                        MATCH (Node() -- Rel(relLabel, hops) -- Node())
                    }

                f relLabel 3u
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

        module ``(RelLabel, uint32 list)`` =

            let rtnSt = "MATCH ()-[:REL_LABEL*0..3]-()"
            let i0 = 0u
            let i3 = 3u

            [<Fact>]
            let ``Variable outside function`` () =

                cypher {
                    MATCH (Node() -- Rel(relLabel, [ i0 .. i3 ]) -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable inside and outside function`` () =

                cypher {
                    MATCH (Node() -- Rel(relLabel, [ 0u .. i3 ]) -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable outside and inside function`` () =

                cypher {
                    MATCH (Node() -- Rel(relLabel, [ i0 .. 3u ]) -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable inside builder`` () =

                cypher {
                    let hops = 3u
                    let min = 0u
                    MATCH (Node() -- Rel(relLabel, [ min .. hops ]) -- Node())
                }
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variables passed as function parameter`` () =
                let f (relLabel : RelLabel) (hops : uint32 list) =
                    cypher {
                         MATCH (Node() -- Rel(relLabel, hops) -- Node())
                    }

                f relLabel [ 0u .. 3u ]
                |> Cypher.query
                |> Query.raw
                |> fun q -> Assert.Equal(rtnSt, q)

    module ``Three Parameter Constructor`` =

        [<Fact>]
        let ``Can make`` () =
            cypher {
                for rel in Graph.RelOfType do
                MATCH (Node() -- Rel(rel, rel.Label, { rel with Value = "NewValue" }) -- Node())
            }
            |> Cypher.query
            |> Query.raw
            |> fun q -> Assert.Equal("""MATCH ()-[rel:REL_LABEL {Value: "NewValue"}]-()""", q)

    module ``RelLabel Combination Operator`` =

        let label = "REL_LABEL"
        let relLabel = RelLabel label / RelLabel label / RelLabel label
        let rtnSt = sprintf "MATCH ()-[:%s|:%s|:%s]-()" label label label

        [<Fact>]
        let ``Create in Rel Constructor`` () =
            cypher {
                MATCH (Node() -- Rel(RelLabel label / RelLabel label / RelLabel label) -- Node())
            }
            |> Cypher.query
            |> Query.raw
            |> fun q -> Assert.Equal(rtnSt, q)

        [<Fact>]
        let ``Variable outside function`` () =

            cypher {
                MATCH (Node() -- Rel relLabel -- Node())
            }
            |> Cypher.query
            |> Query.raw
            |> fun q -> Assert.Equal(rtnSt, q)

        [<Fact>]
        let ``Variable passed as function parameter`` () =
            let f (relLabel : RelLabel) =
                cypher {
                    MATCH (Node() -- Rel relLabel -- Node())
                }

            f relLabel
            |> Cypher.query
            |> Query.raw
            |> fun q -> Assert.Equal(rtnSt, q)

        [<Fact>]
        let ``Variable in statement`` () =
            cypher {
                let relLabel = RelLabel label / RelLabel label / RelLabel label
                MATCH (Node() -- Rel relLabel -- Node())
            }
            |> Cypher.query
            |> Query.raw
            |> fun q -> Assert.Equal(rtnSt, q)

module ``Nodes and Relationships`` =

    [<Fact>]
    let ``MATCH ()-[]-()-[]-()`` () =

        cypher {
            MATCH (Node() -- Rel() -- Node() -- Rel() -- Node())
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("MATCH ()-[]-()-[]-()", q)

    [<Fact>]
    let ``MATCH ()-[]->()-[]-()`` () =

        cypher {
            MATCH (Node() -- Rel() --> Node() -- Rel() -- Node())
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("MATCH ()-[]->()-[]-()", q)

    [<Fact>]
    let ``MATCH ()-[]->()-[]->()`` () =

        cypher {
            MATCH (Node() -- Rel() --> Node() -- Rel() --> Node())
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("MATCH ()-[]->()-[]->()", q)

    [<Fact>]
    let ``MATCH ()-[]-()-[]->()`` () =

        cypher {
            MATCH (Node() -- Rel() -- Node() -- Rel() --> Node())
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("MATCH ()-[]-()-[]->()", q)

    [<Fact>]
    let ``MATCH ()-[]->()<-[]-()`` () =

        cypher {
            MATCH (Node() -- Rel() --> Node() <-- Rel() -- Node())
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("MATCH ()-[]->()<-[]-()", q)

    [<Fact>]
    let ``MATCH ()..->()<-..() `` () = // Something funny happens if name like rtn
        cypher {
            MATCH (Node() -- Rel() --> Node() <-- Rel() -- Node() -- Rel() --> Node() <-- Rel() -- Node())
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("MATCH ()-[]->()<-[]-()-[]->()<-[]-()", q)

namespace FSharp.Data.Cypher.Test.OPTIONAL_MATCH

open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    [<Fact>]
    let ``Statement`` () =
        cypher {
            OPTIONAL_MATCH (Node() -- Rel () -- Node())
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("OPTIONAL MATCH ()-[]-()", q)

namespace FSharp.Data.Cypher.Test.CREATE

open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    [<Fact>]
    let ``Statement`` () =
        cypher {
            CREATE (Node() -- Rel () -- Node())
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("CREATE ()-[]-()", q)

namespace FSharp.Data.Cypher.Test.MERGE

open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    [<Fact>]
    let ``Statement`` () =
        cypher {
            MERGE (Node() -- Rel () -- Node())
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("MERGE ()-[]-()", q)