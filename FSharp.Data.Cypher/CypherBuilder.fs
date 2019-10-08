namespace FSharp.Data.Cypher

open System
open System.Reflection
open System.Collections
open FSharp.Quotations
open FSharp.Quotations.Patterns
open FSharp.Quotations.DerivedPatterns
open FSharp.Quotations.ExprShape
open FSharp.Quotations.Evaluator
open Neo4j.Driver.V1

type private VarDic = Generic.IReadOnlyDictionary<string,Expr>

type Query<'T> = 
    | NA

type Query =
    static member IsTypeDefOf (o : obj) = 
        let typ = o.GetType()
        typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<Query<_>> 

type private StepBuilder private (stepNo : int, steps : CypherStep list) =
    member _.StepNo = stepNo
    member _.Steps = steps
    member this.Add (step : CypherStep) = StepBuilder(this.StepNo + 1, step :: this.Steps)
    static member Init = StepBuilder(1, [])

type private StatementBuilder(clause: Clause, stepBuilder : StepBuilder) =
    let mutable Prms : ParameterList = []
    let mutable PrmCount : int = 1
    let StepCount = stepBuilder.StepNo
    let ParameterizedSb = new Text.StringBuilder()
    let NonParameterizedSb = new Text.StringBuilder()
    let AddParamterized (str : string) = ParameterizedSb.Append str |> ignore
    let AddNonParamterized (str : string) = NonParameterizedSb.Append str |> ignore

    static member KeySymbol = "$"

    static member FixStringParameter (o : obj) =
        
        let makeStr (s : string) = sprintf "\"%s\"" s

        match o with
        | :? unit as u -> "null"
        | :? string as s -> makeStr s
        | :? bool as b -> b.ToString().ToLower()
        | :? Generic.List<obj> as xs ->
            xs
            |> Seq.map string
            |> String.concat ", "
            |> sprintf "[ %s ]"

        | :? Generic.Dictionary<string, obj> as d ->
            d 
            |> Seq.map (fun kv -> kv.Key + " : " + StatementBuilder.FixStringParameter kv.Value)
            |> String.concat ", "
            |> sprintf "{ %s }"
        | _ -> string o
            
    member private this.Clause = clause

    member this.GenerateKey() = 
        let key = sprintf "step%iparam%i" StepCount PrmCount
        PrmCount <- PrmCount + 1
        AddParamterized StatementBuilder.KeySymbol
        AddParamterized key
        key
    
    member private this.Add (o : obj option) =
        let key = this.GenerateKey()
        match o with
        | Some o -> AddNonParamterized (StatementBuilder.FixStringParameter o)
        | None -> AddNonParamterized "null"
        Prms <- (key, o) :: Prms

    member this.AddObj (o : obj) = 
        Serialization.fixTypes o
        |> this.Add

    member this.AddPrimativeType expr =
        QuotationEvaluator.EvaluateUntyped expr
        |> Serialization.fixTypes
        |> this.Add

    member this.AddSerializedType expr =
        QuotationEvaluator.EvaluateUntyped expr
        :?> IFSEntity
        |> Serialization.serialize
        |> box
        |> Some
        |> this.Add

    member _.AddStatement (stmt : string) =  
        AddNonParamterized stmt
        AddParamterized stmt

    member this.Build = CypherStep(this.Clause, string ParameterizedSb, string NonParameterizedSb, Prms)

module private MatchClause =
    
    // Use these since match statements aren't always happy with typeof<_>
    let typeofNode = typeof<Node>
    let typeofRel = typeof<Rel>
    let typeofIFSNode = typeof<IFSNode>
    let typeofIFSRelationship = typeof<IFSRelationship>
    let typeofNodeLabel = typeof<NodeLabel>
    let typeofNodeLabelList = typeof<NodeLabel list>
    let typeofRelLabel = typeof<RelLabel>
    let typeofUInt32 = typeof<uint32>
    let typeofUInt32List = typeof<uint32 list>
    
    let extractObject (varDic : VarDic) (expr : Expr) =
        match expr with
        | NewObject (_, [ _ ] ) -> QuotationEvaluator.EvaluateUntyped expr
        | PropertyGet (None, _, []) -> QuotationEvaluator.EvaluateUntyped expr
        | Value (obj, _) -> obj
        | Var var -> QuotationEvaluator.EvaluateUntyped varDic.[var.Name]
        | SpecificCall <@ List.map @> (_, _, _)
        | SpecificCall <@ Array.map @> (_, _, _) -> QuotationEvaluator.EvaluateUntyped expr
        | PropertyGet (Some (Var var), pi, []) ->
            let expr = varDic.[var.Name]
            let varObj = QuotationEvaluator.EvaluateUntyped varDic.[var.Name]
            // Need to catch case when there is a let binding to create an object,
            // followed by a call to a property on that object. Need to handle the params
            // order much like in record creation code
            let rec inner expr =
                match expr with
                | Let (_, _, expr) -> inner expr
                | NewRecord (_, _) -> pi.GetValue varObj
                | _ ->
                    // In this case the obj is actually NA of the union type Query<'T> = NA
                    // so will need to create an instance of it and call the static member
                    if Query.IsTypeDefOf varObj
                    then 
                        Deserialization.createNullRecordOrClass var.Type
                        |> pi.GetValue
                    else varObj

            inner expr

        | _ -> sprintf "Could not build Label from expression %A" expr |> invalidOp
    
    // TODO : Need to now parameterize the values
    let makeRecordOrClass (varDic : VarDic) typ (exprs : Expr list) =
        
        let addPrimativeParam (o : obj) = 
            fun fieldName (key : string) -> 
                //fieldName + ": " + key, Serialization.fixTypes o
                
                fieldName + ": " + StatementBuilder.FixStringParameter(o)
                //invalidOp "Not made yet"

        let getValues expr =
            match expr with
            | Var v -> 
                QuotationEvaluator.EvaluateUntyped varDic.[v.Name]
                |> addPrimativeParam
                |> Some
            | Value (o, _) -> addPrimativeParam o |> Some
            | PropertyGet (Some _, _, _) -> None
            | PropertyGet (None, _, _) -> 
                QuotationEvaluator.EvaluateUntyped expr
                |> addPrimativeParam
                |> Some
            | NewUnionCase (ui, _) when Deserialization.isOption ui.DeclaringType ->
                QuotationEvaluator.EvaluateUntyped expr
                |> addPrimativeParam
                |> Some

            | _  -> invalidOp(sprintf "Unmatched Expr when getting field value: %A" expr)

        let fieldValues = List.map getValues exprs
        
        Deserialization.getProperties typ
        |> Array.indexed
        |> Array.choose (fun (i, pi) -> fieldValues.[i] |> Option.map (fun f -> f pi.Name))

    let wrapCreateOfIFS xs =
        xs
        |> Array.toList
        |> List.map (fun f -> f "")
        |> String.concat ", "
        |> sprintf "{%s}"

    let makeIFS (varDic : VarDic) (expr : Expr) =
        let rec inner (expr : Expr) =
            match expr with
            | Coerce (expr, _) -> inner expr
            | Let (_, _, exprNext) -> inner exprNext
            | PropertyGet (None, ifs, []) -> ifs.Name
            | Var ifs -> ifs.Name
            | ValueWithName (_, _, name) -> name
            | NewRecord (t, exprs) -> makeRecordOrClass varDic t exprs |> wrapCreateOfIFS
            | NewObject (_, _) -> invalidOp "Classes are not yet supported" // Hard to support classes..
            | _ -> sprintf "Could not build IFS from expression %A" expr |> invalidOp
        
        inner expr

    let makePathHops (varDic : VarDic) (expr : Expr) =
        
        let makeStatement startValue endValue =
            if endValue = UInt32.MaxValue then sprintf "*%i.." startValue
            else sprintf "*%i..%i" startValue endValue

        let (|GetRange|_|) (expr : Expr) =
            match expr with
            | SpecificCall <@ (..) @> (None, _, [ expr1; expr2 ]) ->
                let startValue = extractObject varDic expr1 :?> uint32
                let endValue = extractObject varDic expr2 :?> uint32
                Some (makeStatement startValue endValue)
 
            | _ -> None

        let (|IsCreateSeq|_|) (expr : Expr) =
            match expr with
            | Call (None, mi, [ Coerce (Call (None, mi2, [ expr ]), _) ]) 
                when mi.Name = "ToList" && mi2.Name = "CreateSequence" -> Some expr
            | _ -> None

        let makeListRng xs = List.min xs, List.max xs

        let makeListFromExpr (expr : Expr) =
            QuotationEvaluator.EvaluateUntyped expr 
            :?> uint32 list 
            |> makeListRng
            ||> makeStatement

        let singleInt i = sprintf "*%i" i

        let (|SingleInt|_|) (expr : Expr) =
            match expr with
            | Var var when var.Type = typeofUInt32 -> QuotationEvaluator.EvaluateUntyped varDic.[var.Name] |> Some
            | Value (o, t) when t =typeofUInt32 -> Some o 
            | PropertyGet (_, pi, _) when pi.PropertyType = typeofUInt32 -> extractObject varDic expr |> Some
            | _ -> None

        let (|IntList|_|) (expr : Expr) =
            match expr with
            | Var var when var.Type = typeofUInt32List -> makeListFromExpr varDic.[var.Name] |> Some
            | Value (_, t) when t = typeofUInt32List -> makeListFromExpr expr |> Some
            | PropertyGet (_, pi, _) when pi.PropertyType = typeofUInt32List -> 
                extractObject varDic expr 
                :?> uint32 list 
                |> makeListRng 
                ||> makeStatement
                |> Some
            | _ -> None

        let (|ListCons|_|) (expr : Expr) =
            match expr with
            | NewUnionCase (ui, _) when ui.Name = "Cons" -> Some (makeListFromExpr expr)
            | _ -> None

        match expr with
        | UInt32 i -> singleInt i
        | SingleInt o -> o :?> uint32 |> singleInt
        | IntList s -> s
        | ListCons statement
        | IsCreateSeq (GetRange statement) -> statement
        | _ -> sprintf "Could not build Path Hops from expression %A" expr |> invalidOp

    let (|NoParams|_|) ((ctrTypes : Type []), (ctrExpr : Expr list)) =    
        match ctrTypes, ctrExpr with
        | [||], [] -> Some ()
        | _ -> None
    
    let (|SingleParam|_|) paramTypes ((ctrTypes : Type []), (ctrExpr : Expr list)) =    
        match ctrTypes, ctrExpr with
        | ctr, [ param ] when ctr = paramTypes -> Some param
        | _ -> None
    
    let (|TwoParams|_|) paramTypes ((ctrTypes : Type []), (ctrExpr : Expr list)) =    
        match ctrTypes, ctrExpr with
        | ctr, [ param1; param2 ] when ctr = paramTypes -> Some (param1, param2)
        | _ -> None
    
    let (|ThreeParams|_|) paramTypes ((ctrTypes : Type []), (ctrExpr : Expr list)) =    
        match ctrTypes, ctrExpr with
        | ctr, [ param1; param2; param3 ] when ctr = paramTypes -> Some (param1, param2, param3)
        | _ -> None

    module Rel =

        let makeRelLabel (varDic : VarDic) (expr : Expr) =
            let rec inner (expr : Expr) =
                match expr with
                | SpecificCall <@ (/) @> (_, _, xs) -> List.sumBy inner xs
                | _ -> extractObject varDic expr :?> RelLabel
            inner expr |> string

        let make (varDic : VarDic) (ctrTypes : Type []) (ctrExpr : Expr list) =
            match ctrTypes, ctrExpr with
            | NoParams -> ""
            | SingleParam [| typeofIFSRelationship |] param -> makeIFS varDic param
            | SingleParam [| typeofRelLabel |] param  -> makeRelLabel varDic param
            | SingleParam [| typeofUInt32 |] param
            | SingleParam [| typeofUInt32List |] param -> makePathHops varDic param
            | TwoParams [| typeofIFSRelationship; typeofRelLabel |] (param1, param2) -> 
                makeIFS varDic param1 + makeRelLabel varDic param2
            | TwoParams [| typeofRelLabel; typeofUInt32 |] (param1, param2) -> 
                makeRelLabel varDic param1 + makePathHops varDic param2
            | TwoParams [| typeofRelLabel; typeofUInt32List |] (param1, param2) -> 
                makeRelLabel varDic param1 + makePathHops varDic param2
            | ThreeParams [| typeofIFSRelationship; typeofRelLabel; typeofIFSRelationship |] (param1, param2, param3) ->
                makeIFS varDic param1 + makeRelLabel varDic param2 + " " + makeIFS varDic param3
            | _ -> sprintf "Unexpected Rel constructor: %A" ctrTypes |> invalidOp
            |> sprintf "[%s]" 

    module Node =

        let makeNodeLabelList (varDic : VarDic) (expr : Expr) =
            match expr with
            | NewUnionCase (ui, _) when ui.Name = "Cons" -> QuotationEvaluator.EvaluateUntyped expr :?> NodeLabel list
            | _ -> extractObject varDic expr :?> NodeLabel list
            |> List.map string 
            |> String.concat ""

        let make (varDic : VarDic) (ctrTypes : Type []) (ctrExpr : Expr list) =
            match ctrTypes, ctrExpr with
            | NoParams -> ""
            | SingleParam [| typeofNodeLabel |] param -> 
                extractObject varDic param :?> NodeLabel |> string
            | SingleParam [| typeofNodeLabelList |] param -> 
                makeNodeLabelList varDic param
            | SingleParam [| typeofIFSNode |] param -> 
                makeIFS varDic param
            | TwoParams [| typeofIFSNode; typeofNodeLabel |] (param1, param2) -> 
                makeIFS varDic param1 + (extractObject varDic param2 :?> NodeLabel |> string)
            | TwoParams [| typeofIFSNode; typeofNodeLabelList |] (param1, param2) -> 
                makeIFS varDic param1 + makeNodeLabelList varDic param2
            | TwoParams [| typeofIFSNode; typeofIFSNode |] (param1, param2) -> 
                makeIFS varDic param1 + " " + makeIFS varDic param2
            | ThreeParams [| typeofIFSNode; typeofNodeLabel; typeofIFSNode |] (param1, param2, param3) -> 
                makeIFS varDic param1 + (extractObject varDic param2 :?> NodeLabel |> string) + " " + makeIFS varDic param3
            | ThreeParams [| typeofIFSNode; typeofNodeLabelList; typeofIFSNode |] (param1, param2, param3) -> 
                makeIFS varDic param1 + makeNodeLabelList varDic param2 + " " + makeIFS varDic param3
            | _ -> sprintf "Unexpected Node constructor: %A" ctrTypes |> invalidOp 
            |> sprintf "(%s)" 

    let (|GetConstructors|_|) fResult (typ : Type) (expr : Expr) =
        let getCtrParamsTypes (ci : ConstructorInfo) = ci.GetParameters() |> Array.map (fun x -> x.ParameterType)
        match expr with
        | NewObject (ci, paramsExpr) when ci.DeclaringType = typ -> Some (fResult (getCtrParamsTypes ci) paramsExpr)
        | _ -> None

    let (|BuildJoin|_|) operatorExpr (symbol : string) fResult expr =
        match expr with
        | SpecificCall operatorExpr (_, _, [ left; right ]) ->
            match left, right with
            | Coerce (NewObject (ci1, _), _), Coerce (NewObject (ci2, _), _) when ci1.DeclaringType = typeofNode && ci2.DeclaringType = typeof<Node> -> 
                if symbol = "<-" then symbol + "-" else "-" + symbol
            | _ -> symbol
            |> fun s -> Some (fResult left + s + fResult right)
        | _ -> None

    let make (stepState : StepBuilder) (clause : Clause) (varDic : VarDic) expr =
        
        let stmBuilder = StatementBuilder(clause, stepState)

        let rec inner (expr : Expr) =
            match expr with
            | GetConstructors (Rel.make varDic) typeofRel statement 
            | GetConstructors (Node.make varDic) typeofNode statement -> statement
            | BuildJoin <@ ( -- ) @> "-" inner statement
            | BuildJoin <@ ( --> ) @> "->" inner statement 
            | BuildJoin <@ ( <-- ) @> "<-" inner statement -> statement
            | Coerce (expr, _)
            | Let (_, _, expr)
            | TupleGet (expr, _)
            | Lambda (_, expr) -> inner expr
            | Var v -> invalidOp (sprintf "You must call Node(..) or Rel(..) for Variable %s within the MATCH statement" v.Name)
            | _ -> invalidOp (sprintf "Unable to build MATCH statement from Exprssion: %A" expr)
    
        inner expr
        |> stmBuilder.AddStatement

        stepState.Add(stmBuilder.Build)
 
module private WhereAndSetStatement =

    let [<Literal>] private IFSEntity = "IFSEntity"

    let make (stepState : StepBuilder) clause (expr : Expr) =

        let stmBuilder = StatementBuilder(clause, stepState)

        let buildState fExpr left symbol right =    
            fExpr left
            stmBuilder.AddStatement symbol
            fExpr right

        let (|Operator|_|) opExpr opSymbol fExpr expr =
            match expr with
            | SpecificCall opExpr (_, _, [ left; right ]) -> 
                Some (buildState fExpr left opSymbol right)
            | _ -> None

        let rec inner (expr : Expr) =
            match expr with
            | Operator <@@ (=) @@> " = " inner finalState
            | Operator <@@ (<) @@> " < " inner finalState
            | Operator <@@ (<=) @@> " <= " inner finalState
            | Operator <@@ (>) @@> " > " inner finalState
            | Operator <@@ (>=) @@> " >= " inner finalState
            | Operator <@@ (<>) @@> " <> " inner finalState -> finalState
            | IfThenElse (left, right, Value(_, _)) -> buildState inner left " AND " right
            | IfThenElse (left, Value(_, _), right) -> buildState inner left " OR " right
            | NewTuple exprs -> // Here for set statement - maybe throw on WHERE?
                exprs
                |> List.iteri (fun i expr -> if i = 0 then inner expr else stmBuilder.AddStatement ", "; inner expr)
            | NewUnionCase (_, [ singleCase ]) -> inner singleCase
            | NewUnionCase (ui, []) when ui.Name = "None" -> stmBuilder.AddPrimativeType expr
            | NewUnionCase (ui, _) when ui.Name = "Cons" || ui.Name = "Empty" -> stmBuilder.AddPrimativeType expr
            | NewArray (_, _) -> stmBuilder.AddPrimativeType expr
            | Value (_, typ) -> 
                if Deserialization.hasInterface typ IFSEntity 
                then stmBuilder.AddSerializedType expr 
                else stmBuilder.AddPrimativeType expr
                
            | PropertyGet (Some (PropertyGet (Some e, pi, _)), _, _) -> 
                inner e |> ignore
                stmBuilder.AddStatement "." |> ignore
                stmBuilder.AddStatement pi.Name
            | PropertyGet (Some e, pi, _) -> 
                inner e |> ignore
                stmBuilder.AddStatement "." |> ignore
                stmBuilder.AddStatement pi.Name
                
            | PropertyGet (None, pi, _) -> 
                if Deserialization.hasInterface pi.PropertyType IFSEntity 
                then stmBuilder.AddSerializedType expr 
                else stmBuilder.AddPrimativeType expr
            | Var v -> stmBuilder.AddStatement v.Name
            | Let (_, _, expr)
            | Lambda (_, expr) -> inner expr
            | _ -> 
                sprintf "Un matched in WHERE/SET statement: %A" expr
                |> invalidOp
        
        inner expr
        stmBuilder.Build
        |> stepState.Add

module private ReturnClause =

    let make<'T> (stepState : StepBuilder) clause (expr : Expr) =
        
        let stmBuilder = StatementBuilder(clause, stepState)
        
        let maker (expr : Expr) =
            match expr with
            | Value (o, _) -> 
                StatementBuilder.FixStringParameter o
                |> stmBuilder.AddStatement
            | Var v -> 
                stmBuilder.AddStatement v.Name
            | PropertyGet (Some (Var v), pi, _) -> 
                stmBuilder.AddStatement v.Name
                stmBuilder.AddStatement "."
                stmBuilder.AddStatement pi.Name
            | PropertyGet (None, pi, _) -> 
                stmBuilder.AddStatement pi.Name
            | _ ->
                exp
                |> sprintf "Trying to extract RETURN statement but couldn't match expression: %A"
                |> invalidOp

            fun (di : Generic.IReadOnlyDictionary<string, obj>) ->
            
                let deserializeIEntity (typ : Type) (returnObj : obj) =
                    returnObj
                    :?> IEntity
                    |> Deserialization.deserialize typ
                    |> fun (typ, xs) -> Deserialization.createRecordOrClass typ xs

                match expr with
                | Value (o, typ) ->
                    let key = StatementBuilder.FixStringParameter o
                    let rtnO = di.[key]
                    let o = Deserialization.fixTypes key typ rtnO
                    Expr.Value(o, typ)
                | Var v -> 
                    let key = v.Name
                    let rtnO = di.[key]
                    let o = deserializeIEntity v.Type rtnO
                    Expr.Value(o, v.Type)
                | PropertyGet (Some (Var v), pi, _) ->
                    let key = sprintf "%s.%s" v.Name pi.Name
                    let rtnO = di.[key]
                    let o = Deserialization.fixTypes pi.Name pi.PropertyType rtnO
                    Expr.Value(o, pi.PropertyType)
                | PropertyGet (None, pi, []) -> 
                    let key = pi.Name
                    let rtnO = di.[key]
                    let o = deserializeIEntity pi.PropertyType rtnO
                    Expr.Value(o, pi.PropertyType)
                | _ ->
                    expr
                    |> sprintf "RETURN. Trying to extract values but couldn't match expression: %A"
                    |> invalidOp


        let rec inner (expr : Expr) =
            match expr with
            | Let (_, _, expr) 
            | Lambda (_, expr) -> inner expr
            | Value _ 
            | Var _ 
            | PropertyGet _ -> maker expr
            | NewTuple exprs ->
                let continuations =
                    exprs
                    |> List.mapi (fun i expr -> 
                        if i <> 0 then stmBuilder.AddStatement ", "
                        maker expr)

                fun di ->
                    continuations
                    |> List.map (fun f -> f di)
                    |> Expr.NewTuple
            | _ -> sprintf "RETURN. Unrecognized expression: %A" expr |> invalidOp
        
        // Need to run so build the statement correctly
        let continuation = inner expr

        let result di = continuation di |> QuotationEvaluator.EvaluateUntyped :?> 'T

        stepState.Add(stmBuilder.Build), result

module private BasicClause =

    let make (stepState : StepBuilder) clause (expr : Expr) =
        
        let stmBuilder = StatementBuilder(clause, stepState)

        let extractStatement (exp : Expr) =
            match exp with
            | Value (o, _) -> stmBuilder.AddObj o
            | Var v -> stmBuilder.AddStatement v.Name
            | PropertyGet (Some (Var v), pi, _) -> 
                stmBuilder.AddStatement v.Name
                stmBuilder.AddStatement "."
                stmBuilder.AddStatement pi.Name
            | PropertyGet (None, pi, _) -> stmBuilder.AddStatement pi.Name
            | _ ->
                exp
                |> sprintf "Trying to extract statement but couldn't match expression: %A"
                |> invalidOp

        let makeTuple (stmBuilder: StatementBuilder) exprs =
            exprs
            |> List.iteri (fun i expr -> 
                if i <> 0 then stmBuilder.AddStatement ", "
                extractStatement expr)

        let rec inner expr =
            match expr with
            | Value _ 
            | Var _ 
            | PropertyGet _ -> extractStatement expr
            | NewTuple exprs -> makeTuple stmBuilder exprs
            | Let (_, _, expr) 
            | Lambda (_, expr) -> inner expr
            | _ -> sprintf "BASIC CLAUSE: Unrecognized expression: %A" expr |> invalidOp

        inner expr
        stepState.Add(stmBuilder.Build)

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
            | NoReturnClause -> invalidOp "You must always return something - use RETURN ()" // Need to improve this as in real life you don't...
            | Continuation c -> c

    type CypherBuilder() =
        
        // Need to define a yield method. It is used rather than zero in case there are variables in scope
        member _.Yield (source : 'T) : Query<'T> = NA

        // f needs a different type 'R to allow multiple for in do loops
        member _.For (source : Query<'T>, f : 'T -> Query<'R>) : Query<'R> = NA 
        
        [<CustomOperation(ClauseNames.MATCH, MaintainsVariableSpace = true)>]
        member _.MATCH (source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSEntity) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.OPTIONAL_MATCH, MaintainsVariableSpace = true)>]
        member _.OPTIONAL_MATCH (source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSEntity) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.CREATE, MaintainsVariableSpace = true)>]
        member _.CREATE (source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSEntity) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.MERGE, MaintainsVariableSpace = true)>]
        member _.MERGE (source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSEntity) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.WHERE, MaintainsVariableSpace = true)>]
        member _.WHERE (source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.SET, MaintainsVariableSpace = true)>]
        member _.SET (source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'T> = NA

        [<CustomOperation(ClauseNames.RETURN, MaintainsVariableSpace = true)>]
        member _.RETURN (source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'R> = NA
        
        [<CustomOperation(ClauseNames.RETURN_DISTINCT, MaintainsVariableSpace = true)>]
        member _.RETURN_DISTINCT (source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'R> = NA
        
        [<CustomOperation(ClauseNames.DELETE, MaintainsVariableSpace = true)>]
        member _.DELETE (source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'R> = NA
        
        [<CustomOperation(ClauseNames.DETACH_DELETE, MaintainsVariableSpace = true)>]
        member _.DETACH_DELETE (source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'R> = NA
        
        // TODO: Can't get the intellisense here by adding in the types as it causes some issues
        [<CustomOperation(ClauseNames.ORDER_BY, MaintainsVariableSpace = true)>]
        member _.ORDER_BY (source : Query<'T>, [<ProjectionParameter>] f) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.DESC, MaintainsVariableSpace = true)>]
        member _.DESC (source : Query<'T>) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.SKIP, MaintainsVariableSpace = true)>]
        member _.SKIP (source : Query<'T>, f : int64) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.LIMIT, MaintainsVariableSpace = true)>]
        member _.LIMIT (source : Query<'T>, f : int64) : Query<'T> = NA
     
    let cypher = CypherBuilder()

    let private run<'T> (expr : Expr) =
        
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

        let (|MatchCreateMerge|_|) (callExpr : Expr) (clause : Clause) (state : StepBuilder) fExpr (expr : Expr) =
            match expr with
            | SpecificCall callExpr (_, _, [ stepAbove; thisStep ]) ->
                let state = MatchClause.make state clause varDic thisStep
                Some (fExpr state stepAbove)
            | _ -> None
                
        let (|WhereSet|_|) (callExpr : Expr) (clause : Clause) (state : StepBuilder) fExpr (expr : Expr) =
            match expr with
            | SpecificCall callExpr (_, _, [ stepAbove; thisStep ]) ->
                let state = WhereAndSetStatement.make state clause thisStep
                Some (fExpr state stepAbove)
            | _ -> None
                
        let mutable returnStatement : ReturnContination<'T> = NoReturnClause
                
        let (|Return|_|) (callExpr : Expr) (clause : Clause) (state : StepBuilder) fExpr (expr : Expr) =
            match expr with
            | SpecificCall callExpr (_, _, [ stepAbove; thisStep ]) ->
                let (state, continuation) = ReturnClause.make<'T> state clause thisStep
                returnStatement <- returnStatement.Update continuation
                Some (fExpr state stepAbove)
            | _ -> None
                
        let (|Basic|_|) (callExpr : Expr) (clause : Clause) (state : StepBuilder) fExpr (expr : Expr) =
            match expr with
            | SpecificCall callExpr (_, _, [ stepAbove; thisStep ]) -> 
                let state = BasicClause.make state clause thisStep
                Some (fExpr state stepAbove)
            | _ -> None
                
        let (|NoStatement|_|) (callExpr : Expr) (clause : Clause) (state : StepBuilder) fExpr (expr : Expr) =
            match expr with
            | SpecificCall callExpr (_, _, [ stepAbove ]) -> 
                let state = state.Add(StatementBuilder(clause, state).Build)
                Some (fExpr state stepAbove)
            | _ -> None

        let buildQry (expr : Expr) =
            let rec inner (state : StepBuilder) (expr : Expr) =
                match expr with
                | SpecificCall <@ cypher.Yield @> _ -> state
                | SpecificCall <@ cypher.For @> (_, _, _) -> state
                | Let (_, _, expr) 
                | Lambda (_, expr) -> inner state expr
                | MatchCreateMerge <@ cypher.MATCH @> MATCH state inner stepList
                | MatchCreateMerge <@ cypher.OPTIONAL_MATCH @> OPTIONAL_MATCH state inner stepList
                | MatchCreateMerge <@ cypher.CREATE @> CREATE state inner stepList
                | MatchCreateMerge <@ cypher.MERGE @> MERGE state inner stepList 
                | WhereSet <@ cypher.WHERE @> WHERE state inner stepList
                | WhereSet <@ cypher.SET @> SET state inner stepList
                | Return <@ cypher.RETURN @> RETURN state inner stepList
                | Return <@ cypher.RETURN_DISTINCT @> RETURN_DISTINCT state inner stepList
                | Basic <@ cypher.DELETE @> DELETE state inner stepList
                | Basic <@ cypher.DETACH_DELETE @> DETACH_DELETE state inner stepList
                | Basic <@ cypher.ORDER_BY @> ORDER_BY state inner stepList
                | NoStatement <@ cypher.DESC @> DESC state inner stepList
                | Basic <@ cypher.SKIP @> SKIP state inner stepList
                | Basic <@ cypher.LIMIT @> LIMIT state inner stepList
                | Basic <@ cypher.DELETE @> DELETE state inner stepList -> stepList
                | _ -> sprintf "Un matched method when building Query: %A" expr |> invalidOp 

            inner StepBuilder.Init expr 

        let stepBuilder = buildQry expr

        Cypher(stepBuilder.Steps, returnStatement.Value)
            
    type CypherBuilder with
        member _.Quote (query : Expr<Query<'T>>) = NA
        member _.Run (expr : Expr<Query<'T>>) = run<'T> expr