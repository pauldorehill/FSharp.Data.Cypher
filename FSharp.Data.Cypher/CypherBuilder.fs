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

type private VarDic = Generic.IReadOnlyDictionary<string,Expr>

type Query<'T> = NA

module private MatchClause =

    let makeLabel (varDic : VarDic) (expr : Expr) =
        match expr with
        | NewObject (_, [ _ ] ) -> QuotationEvaluator.EvaluateUntyped expr
        | PropertyGet (None, _, []) -> QuotationEvaluator.EvaluateUntyped expr
        | Value (obj, _) -> obj
        | Var var -> QuotationEvaluator.EvaluateUntyped varDic.[var.Name]
        | PropertyGet (Some(Var var), pi, []) ->
            let varObj = QuotationEvaluator.EvaluateUntyped varDic.[var.Name]
            // In this case the obj is actually NA of the union type Query<'T> = NA
            // so will need to create an instance of it and call the property
            if varObj.GetType().GetGenericTypeDefinition() = typedefof<Query<_>> 
            then 
                FSharpType.GetRecordFields var.Type
                |> fun obs -> FSharpValue.MakeRecord(var.Type, Array.zeroCreate obs.Length)
                |> pi.GetValue
            else varObj

        | _ -> sprintf "Could not build Label from expression %A" expr |> invalidOp

    let makeIFS (expr : Expr) =
        let rec inner (expr : Expr) =
            match expr with
            | PropertyGet (None, ifs, []) -> ifs.Name
            | Var ifs -> ifs.Name
            | ValueWithName (_, _, name) -> name
            | Coerce (expr, _) -> inner expr
            | _ -> sprintf "Could not build IFS from expression %A" expr |> invalidOp
        
        inner expr

    let (|SingleParam|_|) paramTypes ((ctrTypes : Type []), (ctrExpr : Expr list)) =    
        match ctrTypes, ctrExpr with
        | ctr, [ param ] when ctr = paramTypes -> Some param
        | _ -> None
    
    let (|TwoParams|_|) paramTypes ((ctrTypes : Type []), (ctrExpr : Expr list)) =    
        match ctrTypes, ctrExpr with
        | ctr, [ param1; param2 ] when ctr = paramTypes -> Some(param1, param2)
        | _ -> None
    
    let (|ThreeParams|_|) paramTypes ((ctrTypes : Type []), (ctrExpr : Expr list)) =    
        match ctrTypes, ctrExpr with
        | ctr, [ param1; param2; param3 ] when ctr = paramTypes -> Some(param1, param2, param3)
        | _ -> None

    let typeOfNode = typeof<Node>
    let typeOfRel = typeof<Rel>
    let typeofIFSNode = typeof<IFSNode>
    let typeofIFSRelationship = typeof<IFSRelationship>
    let typeofNodeLabel = typeof<NodeLabel>
    let typeofNodeLabelList = typeof<NodeLabel list>
    let typeofRelLabel = typeof<RelLabel>

    module Rel =

        let makeRelLabel (varDic : VarDic) (expr : Expr) =
            let rec inner (expr : Expr) =
                match expr with
                | SpecificCall <@ (/) @> (_, _, xs) -> List.map inner xs |> String.concat "|"
                | _ -> makeLabel varDic expr :?> RelLabel |> string
                
            inner expr

        let make (varDic : VarDic) (ctrTypes : Type []) (ctrExpr : Expr list) =
            match ctrTypes, ctrExpr with
            | SingleParam [| typeofIFSRelationship |] param -> makeIFS param
            | SingleParam [| typeofRelLabel |] param  -> makeRelLabel varDic param
            | TwoParams [| typeofIFSRelationship; typeofRelLabel |] (param1, param2) -> makeIFS param1 + makeRelLabel varDic param2
            | _ -> sprintf "Unexpected Rel constructor: %A" ctrTypes |> invalidOp
            |> sprintf "[%s]"

    module Node =

        let makeNodeLabelList (varDic : VarDic) (expr : Expr) =
            match expr with
            | NewUnionCase (ui, _) when ui.Name = "Cons" -> QuotationEvaluator.EvaluateUntyped expr :?> NodeLabel list
            | _ -> makeLabel varDic expr :?> NodeLabel list
            |> List.map string 
            |> String.concat ""

        // TODO: This needs some serious attention
        let makeNodeWithProperties e1 e2 =
            invalidOp "TODO: Rewrite node maker with properties"
            //let originalNode = QuotationEvaluator.EvaluateUntyped e1
            //let nodeWithProps = QuotationEvaluator.EvaluateUntyped e2 

            //if originalNode.GetType() <> nodeWithProps.GetType() then invalidOp "Can't compare different types for Node properties"
            //else
            //    let pNames =
            //        originalNode.GetType()
            //        |> FSharp.Reflection.FSharpType.GetRecordFields
            //        |> Array.map (fun x -> x.Name)

            //    let oFields = FSharp.Reflection.FSharpValue.GetRecordFields originalNode
            //    let nFields = FSharp.Reflection.FSharpValue.GetRecordFields nodeWithProps

            //    pNames
            //    |> Array.indexed
            //    |> Array.choose (fun (i, pName) -> 
            //        let o = oFields.[i]
            //        let n = nFields.[i]
            //        if  o = n then None 
            //        // TODO: Combine into the full type checks on properties
            //        else 
            //            if n.GetType() = typeof<string> then sprintf "'%s'" (string n) else string n
            //            |> sprintf "%s : %s" pName
            //            |> Some)
            //    |> String.concat ", "
            //    |> sprintf "{ %s }"

        let make (varDic : VarDic) (ctrTypes : Type []) (ctrExpr : Expr list) =
            match ctrTypes, ctrExpr with
            | [||], [] -> ""
            | SingleParam [| typeofNodeLabel |] param -> makeLabel varDic param :?> NodeLabel |> string
            | SingleParam [| typeofNodeLabelList |] param -> makeNodeLabelList varDic param
            | SingleParam [| typeofIFSNode |] param -> makeIFS param
            | TwoParams [| typeofIFSNode; typeofNodeLabel |] (param1, param2) -> makeIFS param1 + (makeLabel varDic param2 :?> NodeLabel |> string)
            | TwoParams [| typeofIFSNode; typeofNodeLabelList |] (param1, param2) -> makeIFS param1 + makeNodeLabelList varDic param2
            | TwoParams [| typeofIFSNode; typeofIFSNode |] (param1, param2) -> makeNodeWithProperties param1 param2
            | ThreeParams [| typeofIFSNode; typeofNodeLabel; typeofIFSNode |] (param1, param2, param3) -> makeNodeWithProperties param1 param3
            | ThreeParams [| typeofIFSNode; typeofNodeLabelList; typeofIFSNode |] (param1, param2, param3) -> makeNodeWithProperties param1 param3
            | _ -> sprintf "Unexpected Node constructor: %A" ctrTypes |> invalidOp 
            |> sprintf "(%s)"
    
    let (|GetConstructors|_|) (typ : Type) (expr : Expr) =
        let getCtrParamsTypes (ci : ConstructorInfo) = ci.GetParameters() |> Array.map (fun x -> x.ParameterType)
        match expr with
        | NewObject (ci, paramsExpr) when ci.DeclaringType = typ -> Some(getCtrParamsTypes ci, paramsExpr)
        | _ -> None

    let (|BuildJoin|_|) operator (symbol : string) expr =
        match expr with
        | SpecificCall operator (_, _, [ left; right ]) ->
            match left, right with
            | Coerce (NewObject (ci1, _), _), Coerce (NewObject (ci2, _), _) 
                when ci1.DeclaringType = typeof<Node> && ci2.DeclaringType = typeof<Node> -> if symbol = "<-" then symbol + "-" else "-" + symbol
            | _ -> symbol
            |> fun s -> Some(left, s, right)
        | _ -> None

    let make (varDic : VarDic) expr =
        let rec inner (expr : Expr) =
            match expr with
            | GetConstructors typeOfRel (ctrTypes, exprList) -> Rel.make varDic ctrTypes exprList
            | GetConstructors typeOfNode (ctrTypes, exprList) -> Node.make varDic ctrTypes exprList
            | BuildJoin <@ (--) @> "-" (left, sym, right)
            | BuildJoin <@ (-->) @> "->" (left, sym, right) 
            | BuildJoin <@ (<--) @> "<-" (left, sym, right) -> inner left + sym + inner right
            | Coerce (expr, _)
            | Let (_, _, expr)
            | TupleGet (expr, _)
            | Lambda (_, expr) -> inner expr
            | _ -> sprintf "Unable to build MATCH statement: %A" expr |> invalidOp
    
        inner expr
 
module private ClauseHelpers =  
    
    let makePropertyKey (v : Var) (pi : PropertyInfo) = v.Name + "." + pi.Name

    let extractStatement (exp : Expr) =
        match exp with
        | Value (o, typ) -> CypherStep.FixStringParameter(typ, o)
        | Var v -> v.Name
        | PropertyGet (Some(Var v), pi, _) -> makePropertyKey v pi
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

        let (|Operator|_|) opExpr opSymbol expr =
            match expr with
            | SpecificCall opExpr (_, _, [ left; right ]) -> Some(left, [ Choice1Of2(sprintf " %s " opSymbol) ], right)
            | _ -> None

        let rec inner (e : Expr) =
            match e with
            | Operator <@@ (=) @@> "=" (left, symbol, right)
            | Operator <@@ (<) @@> "<" (left, symbol, right)
            | Operator <@@ (<=) @@> "<=" (left, symbol, right)
            | Operator <@@ (>) @@> ">" (left, symbol, right)
            | Operator <@@ (>=) @@> ">=" (left, symbol, right)
            | Operator <@@ (<>) @@> "<>" (left, symbol, right) -> inner left @ symbol @ inner right 
            | IfThenElse (left, right, Value(_, _)) -> inner left @ [ Choice1Of2 " AND " ] @ inner right // Value(false) for AND
            | IfThenElse (left, Value(_, _), right) -> inner left @ [ Choice1Of2 " OR " ] @ inner right // Value(true) for OR
            | NewTuple exprs -> // Here for set statement - maybe throw on WHERE?
                let len = List.length exprs
                exprs 
                |> List.mapi (fun i e -> if i = len - 1 then inner e else inner e @ [ Choice1Of2 ", " ])
                |> List.concat
                
            | NewUnionCase (ui, [singleCase]) -> inner singleCase
            | NewUnionCase (ui, _) when ui.Name = "Cons" || ui.Name = "Empty" -> [ QuotationEvaluator.EvaluateUntyped e |> addPrimativeParam ]
            | NewArray (_, _) -> [ QuotationEvaluator.EvaluateUntyped e |> addPrimativeParam ] 
            //| ValueWithName (o, typ, _) -> if Deserialization.hasInterface typ IFSEntity then [ addSerializedParam e ] else [ addPrimativeParam o ]
            | Value (o, typ) -> if Deserialization.hasInterface typ IFSEntity then [ addSerializedParam e ] else [ addPrimativeParam o ] // Also matches ValueWithName
            | PropertyGet (Some(PropertyGet (Some e, pi, _)), _, _) -> inner e @  Choice1Of2 "." :: [ Choice1Of2 pi.Name ] // Deeper than single "." used for options .Value
            | PropertyGet (Some e, pi, _) -> inner e @  Choice1Of2 "." :: [ Choice1Of2 pi.Name ]
            | PropertyGet (None, pi, _) -> 
                if Deserialization.hasInterface pi.PropertyType IFSEntity 
                then [ addSerializedParam e ] 
                else  [ QuotationEvaluator.EvaluateUntyped e |> addPrimativeParam ] //[ Choice1Of2 pi.Name ]
            | Var v -> [ Choice1Of2 v.Name ]
            | Let (_, _, expr)
            | Lambda (_, expr) -> inner expr
            | _ -> 
                sprintf "Un matched in WHERE/SET statement: %A" e
                |> invalidOp
    
        inner e

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

        | PropertyGet (Some(Var v), pi, _) ->
            let key = makePropertyKey v pi
            let o = Deserialization.fixTypes pi.Name pi.PropertyType di.[key]
            Expr.Value(o, pi.PropertyType)
        | PropertyGet (None, pi, []) -> 
            Expr.Value(makeNewType pi.Name exp.Type di, pi.PropertyType)
        | _ ->
            exp
            |> sprintf "RETURN. Trying to extract values but couldn't match expression: %A"
            |> invalidOp

    let make<'T> (expr : Expr) =
        let rec inner (expr : Expr) =
            match expr with
            | Value _ | Var _ | PropertyGet _ ->
                let statement = extractStatement expr
                let continuation di = extractValue di expr
                statement, continuation

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

            | Let (_, _, expr) 
            | Lambda (_, expr) -> inner expr
            | _ -> sprintf "RETURN. Unrecognized expression: %A" expr |> invalidOp
        
        inner expr
        |> fun (statement, continuation) ->
            let result di = continuation di |> QuotationEvaluator.EvaluateUntyped :?> 'T
            statement, result

module private BasicClause =
    
    open ClauseHelpers

    let rec make (expr : Expr) =
        match expr with
        | Value _ 
        | Var _ 
        | PropertyGet _ -> extractStatement expr
        | NewTuple exprs ->
            exprs
            |> List.map extractStatement
            |> String.concat ", "
        | Let (_, _, expr) 
        | Lambda (_, expr) -> make expr
        | _ -> sprintf "BASIC CLAUSE: Unrecognized expression: %A" expr |> invalidOp

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
            | NoReturnClause -> invalidOp "You must always return something - use RETURN ()" // Need to improve this
            | Continuation c -> c

    type CypherBuilder() =
        
        // Need to define a yield method. It is used rather than zero in case there are variables in scope
        member _.Yield(source : 'T) : Query<'T> = NA

        // f needs a different type 'R to allow multiple for in do loops
        member _.For(source : Query<'T>, f : 'T -> Query<'R>) : Query<'R> = NA 
        
        [<CustomOperation(ClauseNames.MATCH, MaintainsVariableSpace = true)>]
        member _.MATCH(source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSEntity) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.OPTIONAL_MATCH, MaintainsVariableSpace = true)>]
        member _.OPTIONAL_MATCH(source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSEntity) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.CREATE, MaintainsVariableSpace = true)>]
        member _.CREATE(source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSEntity) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.MERGE, MaintainsVariableSpace = true)>]
        member _.MERGE(source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSEntity) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.WHERE, MaintainsVariableSpace = true)>]
        member _.WHERE(source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.SET, MaintainsVariableSpace = true)>]
        member _.SET(source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'T> = NA

        [<CustomOperation(ClauseNames.RETURN, MaintainsVariableSpace = true)>]
        member _.RETURN(source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'R> = NA
        
        [<CustomOperation(ClauseNames.RETURN_DISTINCT, MaintainsVariableSpace = true)>]
        member _.RETURN_DISTINCT(source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'R> = NA
        
        [<CustomOperation(ClauseNames.DELETE, MaintainsVariableSpace = true)>]
        member _.DELETE(source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'R> = NA
        
        [<CustomOperation(ClauseNames.DETACH_DELETE, MaintainsVariableSpace = true)>]
        member _.DETACH_DELETE(source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'R> = NA
        
        // Can't get this to work with intellisense
        // Might need to change the type to have 2 x generics so can carry the final type
        [<CustomOperation(ClauseNames.ORDER_BY, MaintainsVariableSpace = true)>]
        member _.ORDER_BY(source : Query<'T>, [<ProjectionParameter>] f) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.SKIP, MaintainsVariableSpace = true)>]
        member _.SKIP(source : Query<'T>, f : int64) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.LIMIT, MaintainsVariableSpace = true)>]
        member _.LIMIT(source : Query<'T>, f : int64) : Query<'T> = NA

        member _.Quote(query : Expr<Query<'T>>) = NA

        member cypher.Run(expr : Expr<Query<'T>>) = 
            // TODO: This is a bit rough and ready
            let varDic =
                let mutable varExp = None
                let varDic = Generic.Dictionary<string,Expr>()
                let rec inner expr =
                    match expr with
                    | SpecificCall <@ cypher.Yield @> _ -> ()
                    | SpecificCall <@ cypher.For @> (_, _, [ varValue; yieldEnd ]) ->
                        varExp <- Some varValue
                        inner yieldEnd
                    | Let (v, e1, e2) -> 
                        if not(varDic.ContainsKey v.Name) then
                            varDic.Add(v.Name, if varExp.IsSome then varExp.Value else e1)
                            varExp <- None
                        inner e2
                    | ShapeCombination (_, exprs) -> List.iter inner exprs
                    | ShapeLambda (_, expr) -> inner expr
                    | ShapeVar _ -> ()
                inner expr
                varDic

            let (|MatchCreateMerge|_|) (callExpr : Expr) (clause : Clause) (state : CypherStep list) (expr : Expr) =
                match expr with
                | SpecificCall callExpr (_, _, [ stepAbove; thisStep ]) ->
                    let statement = MatchClause.make varDic thisStep
                    let newState = NonParameterized (clause, statement) :: state
                    Some(newState, stepAbove)
                | _ -> None
            
            let (|WhereSet|_|) (callExpr : Expr) (clause : Clause) (state : CypherStep list) (expr : Expr) =
                match expr with
                | SpecificCall callExpr (_, _, [ stepAbove; thisStep ]) ->
                    let statement = WhereAndSetStatement.make thisStep
                    let newState = Parameterized(clause, statement) :: state
                    Some(newState, stepAbove)
                | _ -> None
            
            let mutable returnStatement : ReturnContination<'T> = NoReturnClause
            
            let (|Return|_|) (callExpr : Expr) (clause : Clause) (state : CypherStep list) (expr : Expr) =
                match expr with
                | SpecificCall callExpr (_, _, [ stepAbove; thisStep ]) -> 
                    let (statement, continuation) = ReturnClause.make<'T> thisStep
                    let newState = NonParameterized(clause, statement) :: state
                    returnStatement <- returnStatement.Update continuation
                    Some(newState, stepAbove)
                | _ -> None
            
            let (|Basic|_|) (callExpr : Expr) (clause : Clause) (state : CypherStep list) (expr : Expr) =
                match expr with
                | SpecificCall callExpr (_, _, [ stepAbove; thisStep ]) -> 
                    let statement = BasicClause.make thisStep
                    let newState = NonParameterized(clause, statement) :: state
                    Some(newState, stepAbove)
                | _ -> None

            let buildQry (expr : Expr<Query<'T>>) =
                let rec inner (state : CypherStep list) (expr : Expr) =
                    match expr with
                    | SpecificCall <@ cypher.Yield @> _ -> state
                    | SpecificCall <@ cypher.For @> (_, _, _) -> state
                    | Let (_, _, expr) 
                    | Lambda (_, expr) -> inner state expr
                    | MatchCreateMerge <@ cypher.MATCH @> MATCH state (newState, stepAbove)
                    | MatchCreateMerge <@ cypher.OPTIONAL_MATCH @> OPTIONAL_MATCH state (newState, stepAbove)
                    | MatchCreateMerge <@ cypher.CREATE @> CREATE state (newState, stepAbove)
                    | MatchCreateMerge <@ cypher.MERGE @> MERGE state (newState, stepAbove) 
                    | WhereSet <@ cypher.WHERE @> WHERE state (newState, stepAbove)
                    | WhereSet <@ cypher.SET @> SET state (newState, stepAbove)
                    | Return <@ cypher.RETURN @> RETURN state (newState, stepAbove)
                    | Return <@ cypher.RETURN_DISTINCT @> RETURN_DISTINCT state (newState, stepAbove)
                    | Basic <@ cypher.DELETE @> DELETE state (newState, stepAbove)
                    | Basic <@ cypher.DETACH_DELETE @> DETACH_DELETE state (newState, stepAbove)
                    | Basic <@ cypher.ORDER_BY @> ORDER_BY state (newState, stepAbove)
                    | Basic <@ cypher.SKIP @> SKIP state (newState, stepAbove)
                    | Basic <@ cypher.LIMIT @> LIMIT state (newState, stepAbove)
                    | Basic <@ cypher.DELETE @> DELETE state (newState, stepAbove) -> inner newState stepAbove
                    | _ -> sprintf "Un matched method when building Query: %A" expr |> invalidOp 

                inner [] expr, returnStatement.Value
            
            Cypher(buildQry expr)

    let cypher = CypherBuilder()