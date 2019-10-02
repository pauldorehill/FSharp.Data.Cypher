namespace FSharp.Data.Cypher.Test.Match

open System
open FSharp.Data.Cypher
open FSharp.Data.Cypher.Test
open Xunit


module Node =

    let node = { new IFSNode }

    let label = "NodeLabel"

    let nodeLabel = NodeLabel label

    let nodeLabels = [ NodeLabel label; NodeLabel label; NodeLabel label ]

    type NodeType =
        { 
          IntValue : int
          StringValue : string
          
          FloatValue : float }
        interface IFSNode
        member _.Label = NodeLabel label
        static member StaticLabel = NodeLabel label

    type Graph =
        static member Node : Query<IFSNode> = NA
        static member NodeOfType : Query<NodeType> = NA

    module ``Empty Constructor`` =
        
        let rtnSt = sprintf "MATCH %s"

        [<Fact>]
        let ``Single Node`` () =

            cypher {
                MATCH (Node())
                RETURN ()
            }
            |> Cypher.queryNonParameterized
            |> fun q -> Assert.Equal(rtnSt "()", q)
            
        [<Fact>]
        let ``Two Nodes --`` () =

            cypher {
                MATCH (Node() -- Node())
                RETURN ()
            }
            |> Cypher.queryNonParameterized
            |> fun q -> Assert.Equal(rtnSt "()--()", q)
            
        [<Fact>]
        let ``Two Nodes -->`` () =

            cypher {
                MATCH (Node() --> Node())
                RETURN ()
            }
            |> Cypher.queryNonParameterized
            |> fun q -> Assert.Equal(rtnSt "()-->()", q)
            
        [<Fact>]
        let ``Two Nodes <--`` () =

            cypher {
                MATCH (Node() <-- Node())
                RETURN ()
            }
            |> Cypher.queryNonParameterized
            |> fun q -> Assert.Equal(rtnSt "()<--()", q)
    
    module ``Single Parameter Constructor`` =

        module ``NodeLabel`` =

            let rtnSt = sprintf "MATCH (:%s)" label
        
            [<Fact>]
            let ``Create in Node Constructor`` () =
                cypher {
                    MATCH (Node(NodeLabel label))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable outside function`` () =

                cypher {
                    MATCH (Node nodeLabel)
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable passed as function parameter`` () =
                let f (nodeLabel : NodeLabel) =
                    cypher {
                        MATCH (Node nodeLabel)
                        RETURN ()
                    }

                f nodeLabel
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)
        
            [<Fact>]
            let ``Variable in statement`` () =
                cypher {
                    let nodeLabel = NodeLabel label
                    MATCH (Node nodeLabel)
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)
        
        module ``NodeLabel list`` =
        
            let nodeLabels = [ NodeLabel label; NodeLabel label; NodeLabel label]
            let rtnSt = sprintf "MATCH (:%s:%s:%s)" label label label
        
            [<Fact>]
            let ``Create in Node Constructor`` () =
                cypher {
                    MATCH (Node([ NodeLabel label; NodeLabel label; NodeLabel label]))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable outside function`` () =

                cypher {
                    MATCH (Node nodeLabels)
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable passed as function parameter`` () =
                let f (nodeLabels : NodeLabel list) =
                    cypher {
                        MATCH (Node nodeLabels)
                        RETURN ()
                    }

                f nodeLabels
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)
        
            [<Fact>]
            let ``Variable in statement`` () =
                cypher {
                    let nodeLabels = [ NodeLabel label; NodeLabel label; NodeLabel label]
                    MATCH (Node nodeLabels)
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

        module ``IFSNode`` =

            let rtnSt = "MATCH (node)"
            // Quotations can't contain object expressions
            // so use a graph for binding test

            [<Fact>]
            let ``Variable outside function`` () =
                cypher {
                    MATCH (Node node)
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable passed as function parameter`` () =
                let f (node : IFSNode) =
                    cypher {
                        MATCH (Node node)
                        RETURN ()
                    }

                f node
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)
        
            [<Fact>]
            let ``For in do statement`` () =
                cypher {
                    for node in Graph.Node do
                    MATCH (Node node)
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

    module ``Two Parameter Constructor`` =

        module ``(IFSNode, NodeLabel)`` =
        
            let rtnSt = "MATCH (node:NodeLabel)"

            [<Fact>]
            let ``Variable outside function`` () =

                cypher {
                    MATCH (Node(node, nodeLabel))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable passed as function parameter`` () =
                let f (node : IFSNode) (nodeLabel : NodeLabel) =
                    cypher {
                        MATCH (Node(node, nodeLabel))
                        RETURN ()
                    }

                f node nodeLabel
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ```For in do statement`` () =
                cypher {
                    for node in Graph.NodeOfType do
                    MATCH (Node(node, node.Label))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

        module ``(IFSNode, NodeLabel List)`` =
        
            let nodeLabels = [ NodeLabel label; NodeLabel label; NodeLabel label]

            let rtnSt = sprintf "MATCH (node:%s:%s:%s)" label label label
        
            [<Fact>]
            let ``Create in Node Constructor`` () =
                cypher {
                    MATCH (Node(node, [ NodeLabel label; NodeLabel label; NodeLabel label]))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Node labels with List.map`` () =
                cypher {
                    MATCH (Node(node, List.map NodeLabel [ label; label; label ]))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)
            
            [<Fact>]
            let ``Let binding and Node labels with List.map`` () =
                cypher {
                    let labels = List.map NodeLabel [ label; label; label ]
                    MATCH (Node(node, labels))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable outside function`` () =

                cypher {
                    MATCH (Node(node, nodeLabels))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable passed as function parameter`` () =
                let f (node : IFSNode) (nodeLabels : NodeLabel list) =
                    cypher {
                        MATCH (Node(node, nodeLabels))
                        RETURN ()
                    }

                f node nodeLabels
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)
        
            [<Fact>]
            let ``Variable in statement`` () =
                cypher {
                    let nodeLabels = [ NodeLabel label; NodeLabel label; NodeLabel label]
                    MATCH (Node(node, nodeLabels))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

        module ``(IFSNode, IFSNode)`` =
            
            let rtnStSingleProp = """MATCH (node { StringValue : "NewStringValue" })"""

            [<Fact>]
            let ``For in do statement`` () =
                cypher {
                    for node in Graph.NodeOfType do
                    MATCH (Node(node, { node with IntValue = 3; FloatValue = 2.0; StringValue = "NewStringValue" }))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnStSingleProp, q)

    

// TODO


// Two - All
// Three - All

module Relationship =
    
    let rel = { new IFSRelationship }

    let label = "REL_LABEL"

    let relLabel = RelLabel label

    type RelType =
        { Value : string }
        interface IFSRelationship
        member _.Label = RelLabel label
        static member StaticLabel = RelLabel label
        member _.IntLabel = 3u
        static member IntLabelList = [ 0u .. 3u ]

    type Graph =
        static member Rel : Query<IFSRelationship> = NA
        static member RelOfType : Query<RelType> = NA

    module ``Empty Constructor`` =
        
        [<Fact>]
        let ``Blank relationship`` () =

            cypher {
                MATCH (Rel())
                RETURN ()
            }
            |> Cypher.queryNonParameterized
            |> fun q -> Assert.Equal("MATCH []", q)

    module ``Single Parameter Constructor`` =

        module ``IFSRelationship`` =

            let rtnSt = "MATCH [rel]"
            // Quotations can't contain object expressions
            // so use a graph for binding test

            [<Fact>]
            let ``Variable outside function`` () =
                cypher {
                    MATCH (Rel rel)
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable passed as function parameter`` () =
                let f (rel : IFSRelationship) =
                    cypher {
                        MATCH (Rel rel)
                        RETURN ()
                    }

                f rel
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)
        
            [<Fact>]
            let ``For in do statement`` () =
                cypher {
                    for rel in Graph.Rel do
                    MATCH (Rel rel)
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

        module ``RelLabel`` =
            
            let rtnSt = sprintf "MATCH [:%s]" label

            [<Fact>]
            let ``Create in Rel Constructor`` () =
                cypher {
                    MATCH (Rel(RelLabel label))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable outside function`` () =

                cypher {
                    MATCH (Rel relLabel)
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable passed as function parameter`` () =
                let f (relLabel : RelLabel) =
                    cypher {
                        MATCH (Rel relLabel)
                        RETURN ()
                    }

                f relLabel
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)
        
            [<Fact>]
            let ``Variable in statement`` () =
                cypher {
                    let relLabel = RelLabel label
                    MATCH (Rel relLabel)
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)
                
            [<Fact>]
            let ``For .. in with member`` () =
                cypher {
                    for rel in Graph.RelOfType do
                    MATCH (Rel rel.Label)
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)
                
            [<Fact>]
            let ``For .. in with Static member`` () =
                cypher {
                    MATCH (Rel RelType.StaticLabel)
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)
            
        module ``Path Hops`` =

            [<Fact>]
            let ``Fixed no of path hops`` () =
                cypher {
                    MATCH (Rel(3u))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal("MATCH [*3]", q)
            
            [<Fact>]
            let ``Fixed no of path hops on static member`` () =
                cypher {
                    MATCH (Rel RelType.IntLabelList)
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal("MATCH [*0..3]", q)
            
            [<Fact>]
            let ``Fixed no of path hops on for in`` () =
                cypher {
                    for rel in Graph.RelOfType do
                    MATCH (Rel rel.IntLabel)
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal("MATCH [*3]", q)
            
            [<Fact>]
            let ``Range of path hops`` () =
                cypher {
                    MATCH (Rel([ 1u .. 3u ]))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal("MATCH [*1..3]", q)
            
            [<Fact>]
            let ``Range of path hops: Max path hops`` () =
                cypher {
                    MATCH (Rel([ 0u .. UInt32.MaxValue ]))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal("MATCH [*0..]", q)
            
            [<Fact>]
            let ``List literal`` () =
                cypher {
                    MATCH (Rel([ 1u; 0u; 3u; 5u; 3u; 7u; ]))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal("MATCH [*0..7]", q)

    module ``Two Parameter Constructor`` =

        module ``(IFSRelationship, RelLabel)`` =
        
            let rtnSt = "MATCH [rel:REL_LABEL]"

            [<Fact>]
            let ``Variable outside function`` () =

                cypher {
                    MATCH (Rel(rel, relLabel))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable passed as function parameter`` () =
                let f (rel : IFSRelationship) (relLabel : RelLabel) =
                    cypher {
                        MATCH (Rel(rel, relLabel))
                        RETURN ()
                    }

                f rel relLabel
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``For .. in with member`` () =
                cypher {
                    for rel in Graph.RelOfType do
                    MATCH (Rel(rel, rel.Label))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

        module ``(RelLabel, uint32)`` =
        
            let rtnSt = "MATCH [:REL_LABEL*3]"
            let i = 3u

            [<Fact>]
            let ``Variable outside function`` () =

                cypher {
                    MATCH (Rel(relLabel, 3u))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable inside builder`` () =

                cypher {
                    let hops = 3u
                    MATCH (Rel(relLabel, hops))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variables passed as function parameter`` () =
                let f (relLabel : RelLabel) (hops : uint32) =
                    cypher {
                        MATCH (Rel(relLabel, hops))
                        RETURN ()
                    }

                f relLabel 3u 
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)
        
        module ``(RelLabel, uint32 list)`` =
        
            let rtnSt = "MATCH [:REL_LABEL*0..3]"
            let i0 = 0u
            let i3 = 3u

            [<Fact>]
            let ``Variable outside function`` () =

                cypher {
                    MATCH (Rel(relLabel, [ i0 .. i3 ]))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)
            
            [<Fact>]
            let ``Variable inside and outside function`` () =

                cypher {
                    MATCH (Rel(relLabel, [ 0u .. i3 ]))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)
            
            [<Fact>]
            let ``Variable outside and inside function`` () =

                cypher {
                    MATCH (Rel(relLabel, [ i0 .. 3u ]))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variable inside builder`` () =

                cypher {
                    let hops = 3u
                    let min = 0u
                    MATCH (Rel(relLabel, [ min .. hops ]))
                    RETURN ()
                }
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

            [<Fact>]
            let ``Variables passed as function parameter`` () =
                let f (relLabel : RelLabel) (hops : uint32 list) =
                    cypher {
                         MATCH (Rel(relLabel, hops))
                         RETURN ()
                    }

                f relLabel [ 0u .. 3u ] 
                |> Cypher.queryNonParameterized
                |> fun q -> Assert.Equal(rtnSt, q)

    module ``RelLabel Combination Operator`` =
        
        let label = "REL_LABEL"
        let relLabel = RelLabel label / RelLabel label / RelLabel label
        let rtnSt = sprintf "MATCH [:%s|:%s|:%s]" label label label
        
        [<Fact>]
        let ``Create in Rel Constructor`` () =
            cypher {
                MATCH (Rel(RelLabel label / RelLabel label / RelLabel label))
                RETURN ()
            }
            |> Cypher.queryNonParameterized
            |> fun q -> Assert.Equal(rtnSt, q)

        [<Fact>]
        let ``Variable outside function`` () =

            cypher {
                MATCH (Rel relLabel)
                RETURN ()
            }
            |> Cypher.queryNonParameterized
            |> fun q -> Assert.Equal(rtnSt, q)

        [<Fact>]
        let ``Variable passed as function parameter`` () =
            let f (relLabel : RelLabel) =
                cypher {
                    MATCH (Rel relLabel)
                    RETURN ()
                }

            f relLabel
            |> Cypher.queryNonParameterized
            |> fun q -> Assert.Equal(rtnSt, q)
        
        [<Fact>]
        let ``Variable in statement`` () =
            cypher {
                let relLabel = RelLabel label / RelLabel label / RelLabel label
                MATCH (Rel relLabel)
                RETURN ()
            }
            |> Cypher.queryNonParameterized
            |> fun q -> Assert.Equal(rtnSt, q)
