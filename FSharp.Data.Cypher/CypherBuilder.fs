namespace FSharp.Data.Cypher

open System
open System.Reflection
open System.Collections
open FSharp.Reflection
open FSharp.Quotations
open FSharp.Quotations.Patterns
open FSharp.Quotations.DerivedPatterns
open FSharp.Quotations.ExprShape
open FSharp.Quotations.Evaluator
open Neo4j.Driver.V1
open FSharp.Data.Cypher

type private VarDic = Generic.Dictionary<string,Expr>

module private MatchClause =
    
    let getCtrParamsTypes (ci : ConstructorInfo) = ci.GetParameters() |> Array.map (fun x -> x.ParameterType)

    let makeLabel<'T> (varDic : VarDic) (expr : Expr) =
        match expr with
        | NewObject (_, [ _ ] ) -> QuotationEvaluator.EvaluateUntyped expr :?> 'T
        | PropertyGet (None, _, []) -> QuotationEvaluator.EvaluateUntyped expr :?> 'T
        | Value (obj, _) -> obj :?> 'T
        | Var node -> QuotationEvaluator.EvaluateUntyped varDic.[node.Name] :?> 'T
        | _ -> sprintf "Could not build %s from expression %A" typeof<'T>.Name expr |> invalidOp

    module Rel =

        let makeIFSRel (expr : Expr) =
            match expr with
            | PropertyGet (None, rel, []) -> rel.Name
            | Var rel -> rel.Name
            | ValueWithName (_, _, name) -> name
            | _ -> sprintf "Could not build IFSRelationship from expression %A" expr |> invalidOp

        let makeRelLabel (varDic : VarDic) (expr : Expr) =
            let rec inner (expr : Expr) =
                match expr with
                | SpecificCall <@ (/) @> (_, _, xs) -> List.map inner xs |> String.concat "|"
                | _ -> makeLabel<RelLabel> varDic expr |> string
                
            inner expr

        let (|Relationship|_|) (varDic : VarDic) (expr : Expr) =

            let onError prms = sprintf "Unexpected Rel constructor: %A" prms |> invalidOp 

            match expr with
            | NewObject (ci, [ param ]) when ci.DeclaringType = typeof<Rel> -> 
                match getCtrParamsTypes ci with
                | prms when prms = [| typeof<IFSRelationship> |] -> makeIFSRel param
                | prms when prms = [| typeof<RelLabel> |] -> makeRelLabel varDic param
                | errorPrms -> onError errorPrms
                |> sprintf "[%s]"
                |> Some

            | NewObject (ci, [ param1; param2 ]) when ci.DeclaringType = typeof<Rel> ->
                match getCtrParamsTypes ci with
                | prms when prms = [| typeof<IFSRelationship>; typeof<RelLabel> |] -> (makeIFSRel param1, makeRelLabel varDic param2)
                | errorPrms -> onError errorPrms
                ||> sprintf "[%s%s]"
                |> Some

            | _ -> None

    module Node =

        let makeIFSNode (expr : Expr) =
            match expr with
            | Coerce (PropertyGet (_, node, _), _) -> node.Name
            | Coerce (Var node, _) -> node.Name
            | _ -> sprintf "Could not build IFSNode from expression %A" expr |> invalidOp

        let makeNodeLabelList (varDic : VarDic) (expr : Expr) =
            match expr with
            | NewUnionCase (ui, _) when ui.Name = "Cons" -> QuotationEvaluator.EvaluateUntyped expr :?> NodeLabel list
            | _ -> makeLabel<NodeLabel list> varDic expr
            |> List.map string 
            |> String.concat ""

        // TODO: This needs some serious attention
        let makeNodeWithProperties e1 e2 =
            let originalNode = QuotationEvaluator.EvaluateUntyped e1
            let nodeWithProps = QuotationEvaluator.EvaluateUntyped e2 

            if originalNode.GetType() <> nodeWithProps.GetType() then invalidOp "Can't compare different types for Node properties"
            else
                let pNames =
                    originalNode.GetType()
                    |> FSharp.Reflection.FSharpType.GetRecordFields
                    |> Array.map (fun x -> x.Name)

                let oFields = FSharp.Reflection.FSharpValue.GetRecordFields originalNode
                let nFields = FSharp.Reflection.FSharpValue.GetRecordFields nodeWithProps

                pNames
                |> Array.indexed
                |> Array.choose (fun (i, pName) -> 
                    let o = oFields.[i]
                    let n = nFields.[i]
                    if  o = n then None 
                    // TODO: Combine into the full type checks on properties
                    else 
                        if n.GetType() = typeof<string> then sprintf "'%s'" (string n) else string n
                        |> sprintf "%s : %s" pName
                        |> Some)
                |> String.concat ", "
                |> sprintf "{ %s }"

        let (|Node|_|) (varDic : VarDic) (expr : Expr) =

            let onError prms = sprintf "Unexpected Node constructor: %A" prms |> invalidOp 

            match expr with
            | NewObject (ci, []) when ci.DeclaringType = typeof<Node> -> Some "()"

            | NewObject (ci, [ param ]) when ci.DeclaringType = typeof<Node> ->
                match getCtrParamsTypes ci with
                | prms when prms = [| typeof<NodeLabel> |] -> makeLabel<NodeLabel> varDic param |> string
                | prms when prms = [| typeof<NodeLabel list> |] -> makeNodeLabelList varDic param
                | prms when prms = [| typeof<IFSNode> |] -> 
                    makeIFSNode param
                    |> string
                | errorPrms -> onError errorPrms
                |> sprintf "(%s)"
                |> Some

            | NewObject (ci, [ param1; param2 ]) when ci.DeclaringType = typeof<Node> ->
                match getCtrParamsTypes ci  with
                | prms when prms = [| typeof<IFSNode>; typeof<NodeLabel> |] -> makeIFSNode param1 + (makeLabel<NodeLabel> varDic param2 |> string)
                | prms when prms = [| typeof<IFSNode>; typeof<NodeLabel list> |] -> makeIFSNode param1 + makeNodeLabelList varDic param2
                | prms when prms = [| typeof<IFSNode>; typeof<IFSNode> |] -> makeNodeWithProperties param1 param2
                | errorPrms -> onError errorPrms
                |> sprintf "(%s)"
                |> Some

            | NewObject (ci, [ param1; param2; param3 ]) when ci.DeclaringType = typeof<Node> ->
                match getCtrParamsTypes ci  with
                | prms when prms = [| typeof<IFSNode>; typeof<NodeLabel>; typeof<IFSNode> |] -> makeNodeWithProperties param1 param3
                | prms when prms = [| typeof<IFSNode>; typeof<NodeLabel list>; typeof<IFSNode> |] -> makeNodeWithProperties param1 param3
                | errorPrms -> onError errorPrms
                |> sprintf "(%s)"
                |> Some

            | _ -> None

    let getJoinSymbol (onNodes : string) onRel left right =
        match left, right with
        | Coerce (NewObject (ci1, _), _), Coerce (NewObject (ci2, _), _) when ci1.DeclaringType = typeof<Node> && ci2.DeclaringType = typeof<Node> -> onNodes
        | _ -> onRel

    let make (varDic : VarDic) expr =
        let rec inner (expr : Expr) =
            match expr with
            | Rel.Relationship varDic str -> str
            | Node.Node varDic str -> str
            | SpecificCall <@ (--) @> (_, _, [ leftSide; rightSide ]) -> inner leftSide + (getJoinSymbol "--" "-" leftSide rightSide) + inner rightSide
            | SpecificCall <@ (-->) @> (_, _, [ leftSide; rightSide ]) -> inner leftSide + (getJoinSymbol "-->" "->" leftSide rightSide) + inner rightSide
            | SpecificCall <@ (<--) @> (_, _, [ leftSide; rightSide ]) -> inner leftSide + (getJoinSymbol "<--" "<-" leftSide rightSide) + inner rightSide
            | Coerce (expr, _) -> inner expr
            | Let (_, _, expr) -> inner expr
            | TupleGet (expr, _) -> inner expr
            | Lambda (_, expr) -> inner expr
            | _ -> 
                expr
                |> sprintf "MATCH. Unable to build ascii statement: %A"
                |> invalidOp
    
        inner expr
 
module private ClauseHelpers =  
    
    let makePropertyKey (v : Var) (pi : PropertyInfo) = v.Name + "." + pi.Name

    let extractStatement (exp : Expr) =
        match exp with
        | Value (o, typ) -> CypherStep.FixStringParameter(typ, o)
        | Var v -> v.Name
        | PropertyGet (Some (Var v), pi, _) -> makePropertyKey v pi
        | PropertyGet (None, pi, _) -> pi.Name
        | _ ->
            exp
            |> sprintf "Trying to extract statement but couldn't match expression: %A"
            |> invalidOp

module private WhereAndSetStatement =

    let [<Literal>] private IFSEntity = "IFSEntity"

    let make (e : Expr) =
       
        let addPrimativeParam o =
            fun (key : string) -> key, Serialization.fixTypes (o.GetType()) o
            |> Choice2Of2
        
        let addSerializedParam expr =
            let o =
                QuotationEvaluator.EvaluateUntyped expr
                :?> IFSEntity
                |> Serialization.serialize
                |> box
            fun (key : string) -> key, Some o
            |> Choice2Of2

        let rec builder (e : Expr) =
            //printfn "%A" e
            match e with
            | SpecificCall <@@ (=) @@> (_, _, [ left; right ]) -> builder left @ [ Choice1Of2 " = " ] @ builder right
                //match left, right with
                //| Var v, PropertyGet (None, pi, []) -> [ Choice1Of2 v.Name ; Choice1Of2 " = " ; addSerializedParam right ]
                //| PropertyGet (None, pi, []), Var v -> [ addSerializedParam right ; Choice1Of2 " = " ; Choice1Of2 v.Name ]
                //| _ -> builder left @  [ Choice1Of2 " = " ] @ builder right
            | SpecificCall <@@ (<) @@> (_, _, [ left; right ]) -> builder left @ [ Choice1Of2 " < " ] @ builder right
            | SpecificCall <@@ (<=) @@> (_, _, [ left; right ]) -> builder left @ [ Choice1Of2 " <= " ] @ builder right
            | SpecificCall <@@ (>) @@> (_, _, [ left; right ]) -> builder left @ [ Choice1Of2 " > " ] @ builder right
            | SpecificCall <@@ (>=) @@> (_, _, [ left; right ]) -> builder left @ [ Choice1Of2 " >= " ] @ builder right
            | SpecificCall <@@ (<>) @@> (_, _, [ left; right ]) -> builder left @ [ Choice1Of2 " <> " ] @ builder right
            | IfThenElse (left, right, Value (o, t)) -> builder left @ [ Choice1Of2 " AND " ] @ builder right // Value(false) for AND
            | IfThenElse (left, Value (o, t), right) -> builder left @ [ Choice1Of2 " OR " ] @ builder right // Value(true) for OR
            | NewTuple exprs -> // Here for set statement - maybe throw on WHERE?
                let len = List.length exprs
                exprs 
                |> List.mapi (fun i e -> if i = len - 1 then builder e else builder e @ [ Choice1Of2 ", " ])
                |> List.concat
                
            | NewUnionCase (ui, [singleCase]) -> builder singleCase
            | NewUnionCase (ui, _) when ui.Name = "Cons" || ui.Name = "Empty" -> [ QuotationEvaluator.EvaluateUntyped e |> addPrimativeParam ]
            | NewArray (_, _) -> [ QuotationEvaluator.EvaluateUntyped e |> addPrimativeParam ] 
            //| ValueWithName (o, typ, _) -> if Deserialization.hasInterface typ IFSEntity then [ addSerializedParam e ] else [ addPrimativeParam o ]
            | Value (o, typ) -> if Deserialization.hasInterface typ IFSEntity then [ addSerializedParam e ] else [ addPrimativeParam o ] // Also matches ValueWithName
            | PropertyGet (Some (PropertyGet (Some e, pi, _)), _, _) -> builder e @  Choice1Of2 "." :: [ Choice1Of2 pi.Name ] // Deeper than single "." used for options .Value
            | PropertyGet (Some e, pi, _) -> builder e @  Choice1Of2 "." :: [ Choice1Of2 pi.Name ]
            | PropertyGet (None, pi, _) -> 
                if Deserialization.hasInterface pi.PropertyType IFSEntity 
                then [ addSerializedParam e ] 
                else  [ QuotationEvaluator.EvaluateUntyped e |> addPrimativeParam ] //[ Choice1Of2 pi.Name ]
            | Var v -> [ Choice1Of2 v.Name ]
            | Let (_, _, e2) -> builder e2
            | Lambda (_, e) -> builder e
            | _ -> 
                sprintf "Un matched in WHERE/SET statement: %A" e
                |> invalidOp
    
        builder e

module private ReturnClause =

    open ClauseHelpers

    let makeNewType (key : string) (typ : Type) (di : Generic.IReadOnlyDictionary<string, obj>) =
        di.[key] 
        :?> IEntity
        |> Deserialization.deserialize typ
        |> Deserialization.createRecordOrClass typ

    let extractValue (di : Generic.IReadOnlyDictionary<string, obj>) (exp : Expr) =
        match exp with
        | Value (o, typ) ->
            let key = CypherStep.FixStringParameter(typ, o)
            let o = Deserialization.fixTypes key typ di.[key]
            Expr.Value(o, typ)

        | Var v -> 
            // Need to give the compiler some type info for final cast - so return as a value
            // This may be a bit of a hack, but it works!
            Expr.Value(makeNewType v.Name v.Type di, v.Type)

        | PropertyGet (Some (Var v), pi, _) ->
            let key = makePropertyKey v pi
            let o = Deserialization.fixTypes pi.Name pi.PropertyType di.[key]
            Expr.Value(o, pi.PropertyType)
        | PropertyGet (None, pi, []) -> 
            Expr.Value(makeNewType pi.Name exp.Type di, pi.PropertyType)
        | _ ->
            exp
            |> sprintf "RETURN. Trying to extract values but couldn't match expression: %A"
            |> invalidOp

    let make<'T> (e : Expr) =
        let rec inner (exp : Expr) =
            match exp with
            // Single return type
            | Value _ 
            | Var _
            | PropertyGet _ ->
                let statement = extractStatement exp
                let continuation di = extractValue di exp
                statement, continuation

            // Multiple return types
            | NewTuple exprs ->
                let statement = 
                    exprs
                    |> List.map extractStatement
                    |> String.concat ", "

                let contination di =
                    exprs
                    |> List.map (extractValue di)
                    |> Expr.NewTuple

                statement, contination

            | Let (_, _, e2) -> inner e2
            | _ -> 
                exp
                |> sprintf "RETURN. Unrecognized expression: %A"
                |> invalidOp

        match e with
        | Lambda (_, exp) ->
            inner exp
            |> fun (statement, continuation) ->
                let result di =
                    continuation di
                    //|> Linq.RuntimeHelpers.LeafExpressionConverter.EvaluateQuotation
                    |> QuotationEvaluator.EvaluateUntyped :?> 'T
                        
                statement, result
        | _ -> 
            e
            |> sprintf "RETURN. Was not a Lambda : %A"
            |> invalidOp

module private BasicClause =
    
    open ClauseHelpers

    let rec make (exp : Expr) =
        match exp with
        // Single return type
        | Value _ 
        | Var _
        | PropertyGet _ -> extractStatement exp
        // Multiple return types
        | NewTuple exprs ->
            exprs
            |> List.map extractStatement
            |> String.concat ", "

        | Let (_, _, e2) -> make e2
        | Lambda (_, e) -> make e
        | _ -> 
            exp
            |> sprintf "BASIC CLAUSE: Unrecognized expression: %A"
            |> invalidOp

[<AutoOpen>]
module CypherBuilder =

    // Initial help came from this great article by Thomas Petricek
    // http://tomasp.net/blog/2015/query-translation/
    // Other helpful articles
    // https://stackoverflow.com/questions/23122639/how-do-i-write-a-computation-expression-builder-that-accumulates-a-value-and-als
    // https://stackoverflow.com/questions/14110532/extended-computation-expressions-without-for-in-do
    
    type private ReturnContination<'T> =
        | Continuation of (Generic.IReadOnlyDictionary<string, obj> -> 'T)
        | NoReturnClause
        member this.Update v =
            match this with
            | NoReturnClause -> Continuation v
            | Continuation _ -> invalidOp "Only 1 x RETURN clause is allowed in a Cypher query."
        member this.Value =
            match this with
            | NoReturnClause -> invalidOp "You must always return something - use RETURN (unit)"
            | Continuation c -> c


    type Query<'T> = NA

    type CypherBuilder() =
        
        // Need to define a yield method. It is used rather than zero in case there are variables in scope
        member __.Yield(source : 'T) : Query<'T> = NA

        // f needs a different type 'R to allow multiple for in do loops
        member __.For(source : Query<'T>, f : 'T -> Query<'R>) : Query<'R> = NA 
        
        // Cypher Queries
        // All match statements must end in a Node, one bug here if only use a single operator its still returns IFSNode
        [<CustomOperation(ClauseNames.MATCH, MaintainsVariableSpace = true)>]
        member __.MATCH(source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSEntity) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.OPTIONAL_MATCH, MaintainsVariableSpace = true)>]
        member __.OPTIONAL_MATCH(source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSEntity) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.CREATE, MaintainsVariableSpace = true)>]
        member __.CREATE(source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSEntity) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.MERGE, MaintainsVariableSpace = true)>]
        member __.MERGE(source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSEntity) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.WHERE, MaintainsVariableSpace = true)>]
        member __.WHERE(source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.SET, MaintainsVariableSpace = true)>]
        member __.SET(source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'T> = NA

        [<CustomOperation(ClauseNames.RETURN, MaintainsVariableSpace = true)>]
        member __.RETURN(source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'R> = NA
        
        [<CustomOperation(ClauseNames.RETURN_DISTINCT, MaintainsVariableSpace = true)>]
        member __.RETURN_DISTINCT(source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'R> = NA
        
        [<CustomOperation(ClauseNames.DELETE, MaintainsVariableSpace = true)>]
        member __.DELETE(source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'R> = NA
        
        [<CustomOperation(ClauseNames.DETACH_DELETE, MaintainsVariableSpace = true)>]
        member __.DETACH_DELETE(source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'R> = NA
        
        // Can't get this to work with intellisense
        // Might need to change the type to have 2 x generics so can carry the final type
        [<CustomOperation(ClauseNames.ORDER_BY, MaintainsVariableSpace = true)>]
        member __.ORDER_BY(source : Query<'T>, [<ProjectionParameter>] f) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.SKIP, MaintainsVariableSpace = true)>]
        member __.SKIP(source : Query<'T>, f : int64) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.LIMIT, MaintainsVariableSpace = true)>]
        member __.LIMIT(source : Query<'T>, f : int64) : Query<'T> = NA

        member __.Quote(query : Expr<Query<'T>>) = NA

        member cypher.Run(e : Expr<Query<'T>>) = 
            // TODO: This is a bit rough and ready
            let varDic =
                let mutable varExp = None
                let varDic = Generic.Dictionary<string,Expr>()
                let rec inner e =
                    match e with
                    | SpecificCall <@ cypher.Yield @> _ -> ()
                    | SpecificCall <@ cypher.For @> (_, _, [ varValue; yieldEnd ]) ->
                        varExp <- Some varValue
                        inner yieldEnd
                    | Let (v, e1, e2) -> 
                        if not(varDic.ContainsKey v.Name) then
                            varDic.Add(v.Name, if varExp.IsSome then varExp.Value else e1)
                            varExp <- None
                        inner e2
                    | ShapeCombination (_, xs) -> List.iter inner xs
                    | ShapeLambda (_, e) -> inner e
                    | ShapeVar _ -> ()
                        //printfn "Shape %A" e
                        //if varPropGet.IsSome then varDic.Add(v.Name, varPropGet.Value)
                        //varPropGet <- None
                inner e
                varDic
            
            // TODO: should probably pass this down in the state
            let mutable returnStatement : ReturnContination<'T> = NoReturnClause

            let buildQry (e : Expr<Query<'T>>) =
                let rec queryBuilder (state : CypherStep list) (e : Expr) =
                    match e with
                    | SpecificCall <@ cypher.Yield @> _ -> state
                    | SpecificCall <@ cypher.For @> (_, _, _) -> state
                    | Let (v, e1, e2) -> queryBuilder state e2
                    | Lambda (_, exp) -> queryBuilder state exp
                    
                    | SpecificCall <@ cypher.MATCH @> (exp, types, [ stepAbove; thisStep ]) ->
                        let statement = MatchClause.make varDic thisStep
                        queryBuilder state stepAbove |> ignore
                        let newState = NonParameterized(MATCH, statement) :: state
                        queryBuilder newState stepAbove

                    // TODO: This can RETURN some nulls.. need to look further into that
                    | SpecificCall <@ cypher.OPTIONAL_MATCH @> (exp, types, [ stepAbove; thisStep ]) ->
                        let statement = MatchClause.make varDic thisStep
                        let newState = NonParameterized(OPTIONAL_MATCH, statement) :: state
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.CREATE @> (exp, types, [ stepAbove; thisStep ]) ->
                        let statement = MatchClause.make varDic thisStep
                        let newState = NonParameterized(CREATE, statement) :: state
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.MERGE @> (exp, types, [ stepAbove; thisStep ]) ->
                        let statement = MatchClause.make varDic thisStep
                        let newState = NonParameterized(MERGE, statement) :: state
                        queryBuilder newState stepAbove
                    
                    | SpecificCall <@ cypher.WHERE @> (exp, types, [ stepAbove; thisStep ]) ->
                        let statement = WhereAndSetStatement.make thisStep
                        let newState = Parameterized(WHERE, statement) :: state
                        queryBuilder newState stepAbove
                    
                    | SpecificCall <@ cypher.SET @> (exp, types, [ stepAbove; thisStep ]) ->
                        let statement = WhereAndSetStatement.make thisStep
                        let newState = Parameterized(SET, statement) :: state
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.RETURN @> (exp, types, [ stepAbove; thisStep ]) ->
                        let (statement, continuation) = ReturnClause.make<'T> thisStep
                        let newState = NonParameterized(RETURN, statement) :: state
                        returnStatement <- returnStatement.Update continuation
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.RETURN_DISTINCT @> (exp, types, [ stepAbove; thisStep ]) ->
                        let (statement, continuation) = ReturnClause.make<'T> thisStep
                        let newState = NonParameterized(RETURN_DISTINCT, statement) :: state
                        returnStatement <- returnStatement.Update continuation
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.DELETE @> (exp, types, [ stepAbove; thisStep ]) ->
                        let statement = BasicClause.make thisStep
                        let newState = NonParameterized(DELETE, statement) :: state
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.DETACH_DELETE @> (exp, types, [ stepAbove; thisStep ]) ->
                        let statement = BasicClause.make thisStep
                        let newState = NonParameterized(DETACH_DELETE, statement) :: state
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.ORDER_BY @> (exp, types, [ stepAbove; thisStep ]) ->
                        let statement = BasicClause.make thisStep
                        let newState = NonParameterized(ORDER_BY, statement) :: state
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.SKIP @> (exp, types, [ stepAbove; thisStep ]) ->
                        let statement = BasicClause.make thisStep
                        let newState = NonParameterized(SKIP, statement) :: state
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.LIMIT @> (exp, types, [ stepAbove; thisStep ]) ->
                        let statement = BasicClause.make thisStep
                        let newState = NonParameterized(LIMIT, statement) :: state
                        queryBuilder newState stepAbove

                    | _ -> sprintf "Un matched method when building Query: %A" e |> invalidOp 

                queryBuilder [] e, returnStatement.Value
            
            Cypher(buildQry e)

    let cypher = CypherBuilder()