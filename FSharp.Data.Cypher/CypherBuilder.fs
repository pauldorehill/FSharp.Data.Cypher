namespace FSharp.Data.Cypher

open System
open System.Reflection
open System.Collections
open FSharp.Quotations
open FSharp.Quotations.Patterns
open FSharp.Quotations.DerivedPatterns
open FSharp.Quotations.ExprShape
open Neo4j.Driver

type private VarDic = Generic.IReadOnlyDictionary<string,Expr>

module ClauseNames =

    let [<Literal>] ASC = "ASC"
    let [<Literal>] CREATE = "CREATE"
    let [<Literal>] DELETE = "DELETE"
    let [<Literal>] DESC = "DESC"
    let [<Literal>] DETACH_DELETE = "DETACH_DELETE"
    let [<Literal>] FOREACH = "FOREACH"
    let [<Literal>] LIMIT = "LIMIT"
    let [<Literal>] MATCH = "MATCH"
    let [<Literal>] MERGE = "MERGE"
    let [<Literal>] ON_CREATE_SET = "ON_CREATE_SET" // Currently only works with SET
    let [<Literal>] ON_MATCH_SET = "ON_MATCH_SET" // Currently only works with SET
    let [<Literal>] OPTIONAL_MATCH = "OPTIONAL_MATCH"
    let [<Literal>] ORDER_BY = "ORDER_BY"
    let [<Literal>] RETURN = "RETURN"
    let [<Literal>] RETURN_DISTINCT = "RETURN_DISTINCT"
    let [<Literal>] SET = "SET"
    let [<Literal>] SKIP = "SKIP"
    let [<Literal>] UNION = "UNION"
    let [<Literal>] UNION_ALL = "UNION_ALL"
    let [<Literal>] UNWIND = "UNWIND"
    let [<Literal>] WHERE = "WHERE"
    let [<Literal>] WITH = "WITH"

    // TODO - here for write/read check completeness
    let [<Literal>] REMOVE = "REMOVE"
    let [<Literal>] CALL = "CALL"
    let [<Literal>] YIELD = "YIELD"

    // let [<Literal>] CASE = "CASE"  //WHEN //THEN //ELSE //END

type Clause =
    | ASC
    | CREATE
    | DELETE
    | DESC
    | DETACH_DELETE
    | FOREACH
    | LIMIT
    | MATCH
    | MERGE
    | ON_CREATE_SET
    | ON_MATCH_SET
    | OPTIONAL_MATCH
    | ORDER_BY
    | REMOVE
    | RETURN
    | RETURN_DISTINCT
    | SET
    | SKIP
    | UNION
    | UNION_ALL
    | UNWIND
    | WHERE
    | WITH
    override this.ToString() =
        match this with
        | ASC -> ClauseNames.ASC
        | CREATE -> ClauseNames.CREATE
        | DELETE -> ClauseNames.DELETE
        | DESC -> ClauseNames.DESC
        | DETACH_DELETE -> ClauseNames.DETACH_DELETE
        | FOREACH -> ClauseNames.FOREACH
        | LIMIT -> ClauseNames.LIMIT
        | MATCH -> ClauseNames.MATCH
        | MERGE -> ClauseNames.MERGE
        | ON_CREATE_SET -> ClauseNames.ON_CREATE_SET
        | ON_MATCH_SET -> ClauseNames.ON_MATCH_SET
        | OPTIONAL_MATCH -> ClauseNames.OPTIONAL_MATCH
        | ORDER_BY -> ClauseNames.ORDER_BY
        | REMOVE -> ClauseNames.REMOVE
        | RETURN -> ClauseNames.RETURN
        | RETURN_DISTINCT -> ClauseNames.RETURN_DISTINCT
        | SET -> ClauseNames.SET
        | SKIP -> ClauseNames.SKIP
        | UNION -> ClauseNames.UNION
        | UNION_ALL ->ClauseNames.UNION_ALL
        | UNWIND ->ClauseNames.UNWIND
        | WHERE -> ClauseNames.WHERE
        | WITH -> ClauseNames.WITH
        |> fun s -> s.Replace("_", " ")
    member this.IsWrite =
        match this with
        | CREATE | MERGE | SET | DELETE | DETACH_DELETE | REMOVE | FOREACH -> true
        | _ -> false
    member this.IsRead = not (this.IsWrite)

type private Operators =
    | OpEqual
    | OpLess
    | OpGreater
    | OpNotEqual
    | OpLessOrEqual
    | OpGreaterOrEqual
    | OpMM
    | OpLMM
    | OpMMG
    | OpMMMM
    | OpLMMMM
    | OpMMMMG
    member this.Name =
        match this with
        | OpEqual -> "op_Equality"
        | OpLess -> "op_LessThan"
        | OpGreater -> "op_GreaterThan"
        | OpNotEqual -> "op_Inequality"
        | OpLessOrEqual -> "op_LessThanOrEqual"
        | OpGreaterOrEqual -> "op_GreaterThanOrEqual"
        | OpMM -> "op_MinusMinus"
        | OpLMM -> "op_LessMinusMinus"
        | OpMMG -> "op_MinusMinusGreater"
        | OpMMMM -> "op_MinusMinusMinusMinus"
        | OpLMMMM -> "op_LessMinusMinusMinusMinus"
        | OpMMMMG -> "op_MinusMinusMinusMinusGreater"
    override this.ToString() =
        match this with
        | OpMMMM -> "--"
        | OpLMMMM -> "<--"
        | OpMMMMG -> "-->"
        | OpMM -> "-"
        | OpLMM -> "<-"
        | OpMMG -> "->"
        | OpEqual -> "="
        | OpLess -> "<"
        | OpLessOrEqual -> "<="
        | OpGreater -> ">"
        | OpGreaterOrEqual ->">="
        | OpNotEqual -> "<>"

type QuotationEvaluator =
    static member EvaluateUntyped (expr : Expr) = Linq.RuntimeHelpers.LeafExpressionConverter.EvaluateQuotation expr
    static member Evaluate (expr : Expr<'T>) = Linq.RuntimeHelpers.LeafExpressionConverter.EvaluateQuotation expr :?> 'T

[<NoComparison; NoEquality>]
type Query<'T,'Result> = private | NA

type private CypherStep(clause : Clause, statement : string, rawStatement : string, parameters : ParameterList) =
    member _.Clause = clause
    member _.Statement = statement
    member _.RawStatement = rawStatement
    member _.Parameters = parameters

[<Sealed; NoComparison; NoEquality>]
type private StepBuilder private (stepNo : int, stepList : CypherStep list) =
    member _.StepNo = stepNo
    member private this.Steps = stepList
    member this.Add (step : CypherStep) = StepBuilder(this.StepNo + 1, step :: this.Steps)
    static member Init = StepBuilder(1, [])
    member this.Build (continuation : ReturnContination<'T> option) =
        let sb = Text.StringBuilder()
        let makeQuery (paramterized : bool) (multiline : bool) =
            let add (s : string) = sb.Append s |> ignore
            let total = this.Steps.Length
            let mutable count : int = 1

            for step in this.Steps do
                add (string step.Clause)
                if paramterized
                then
                    if step.Statement <> "" then
                        add " "
                        add step.Statement
                else
                    if step.Statement <> "" then
                        add " "
                        add step.RawStatement
                if count < total then
                    if multiline then add Environment.NewLine else add " "
                count <- count + 1

            let qry = string sb
            sb.Clear() |> ignore
            qry

        let parameters = this.Steps |> List.collect (fun cs -> cs.Parameters)
        let query = makeQuery true false
        let queryMultiline = makeQuery true true
        let rawQuery = makeQuery false false
        let rawQueryMultiline = makeQuery false true
        let isWrite = this.Steps |> List.exists (fun x -> x.Clause.IsWrite)
        let continuation = if typeof<'T> = typeof<unit> then None else continuation // If returning unit, no point running the continuation

        Cypher<'T>(continuation, parameters, query, queryMultiline, rawQuery, rawQueryMultiline, isWrite)

type private StatementBuilder(clause: Clause, stepBuilder : StepBuilder) =
    let mutable prms : ParameterList = []
    let mutable prmCount : int = 1
    let stepCount = stepBuilder.StepNo
    let parameterizedSb = Text.StringBuilder()
    let nonParameterizedSb = Text.StringBuilder()
    let addParamterized (str : string) = parameterizedSb.Append str |> ignore
    let addNonParamterized (str : string) = nonParameterizedSb.Append str |> ignore
    let rec fixStringParameter (o : obj) =
        match o with
        | :? unit -> "null"
        | :? string as s -> "\"" + s.Replace("""\""", """\\""") + "\"" // TODO: See https://neo4j.com/docs/cypher-manual/3.5/syntax/expressions/ fro full list
        | :? bool as b -> b.ToString().ToLower()
        | :? Generic.List<obj> as xs ->
            xs
            |> Seq.map string
            |> String.concat ", "
            |> sprintf "[%s]"

        | :? Generic.Dictionary<string, obj> as d ->
            d
            |> Seq.map (fun kv -> kv.Key + ": " + fixStringParameter kv.Value)
            |> String.concat ", "
            |> sprintf "{%s}"
        | _ -> string o

    static member KeySymbol = "$"

    member _.Clause = clause

    member _.GenerateKey() =
        let key = sprintf "step%iparam%i" stepCount prmCount
        prmCount <- prmCount + 1
        addParamterized StatementBuilder.KeySymbol
        addParamterized key
        key

    member this.Add (o : obj option) =
        let key = this.GenerateKey()
        match o with
        | Some o -> addNonParamterized (fixStringParameter o)
        | None -> addNonParamterized "null"
        prms <- (key, o) :: prms
        key

    member this.AddCoreType (o : obj) =
        Serialization.coreTypes o
        |> this.Add
        |> ignore

    member this.AddCoreTypeRtnKey (o : obj) =
        Serialization.coreTypes o
        |> this.Add

    member this.AddCoreType expr =
        QuotationEvaluator.EvaluateUntyped expr
        |> Serialization.coreTypes
        |> this.Add
        |> ignore

    member this.AddComplexType expr =
        QuotationEvaluator.EvaluateUntyped expr
        |> Serialization.complexTypes
        |> box
        |> Some
        |> this.Add
        |> ignore

    member _.AddStatement (stmt : string) =
        addNonParamterized stmt
        addParamterized stmt

    member this.Build = CypherStep(this.Clause, string parameterizedSb, string nonParameterizedSb, prms)

[<NoComparison; NoEquality>]
type AS<'T>() = 
    member _.AS (x : AS<'T>) : 'T = invalidOp "AS.AS should never be called"
    member _.Value : 'T = invalidOp "AS.Value should never be called"

module AggregatingFunctions =
    
    let inline count (x : 'T) = AS<int64>()
    
    let inline collect (x : 'T) = AS<'T list>()

module private  Helpers =
    
    let extractObject (varDic : VarDic) (expr : Expr) =
        match expr with
        | NewObject (_, [ _ ] ) -> QuotationEvaluator.EvaluateUntyped expr
        | PropertyGet (None, _, []) -> QuotationEvaluator.EvaluateUntyped expr
        | Value (obj, _) -> obj
        | Var var -> QuotationEvaluator.EvaluateUntyped varDic.[var.Name]
        | SpecificCall <@@ List.map @@> (_, _, _)
        | SpecificCall <@@ Array.map @@> (_, _, _) -> QuotationEvaluator.EvaluateUntyped expr
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
                    // In this case the obj is actually Node<'N> or Rel<'R>
                    // so will need to create an instance of it and call the static member
                    if Node.IsTypeDefOf varObj || Rel.IsTypeDefOf varObj
                    then
                        Deserialization.createNullRecordOrClass var.Type
                        |> pi.GetValue
                    else varObj

            inner expr

        | _ -> sprintf "Could not build Label from expression %A" expr |> invalidOp

    let (|GetRange|_|) (varDic : VarDic) (expr : Expr) =
        match expr with
        | SpecificCall <@@ (..) @@> (None, _, [ expr1; expr2 ]) ->
            let startValue = extractObject varDic expr1 :?> uint32
            let endValue = extractObject varDic expr2 :?> uint32
            Some (startValue, endValue)
        | _ -> None

    let (|IsCreateSeq|_|) (expr : Expr) =
        match expr with
        | Call (None, mi, [ Coerce (Call (None, mi2, [ expr ]), _) ])
            when mi.Name = "ToList" && mi2.Name = "CreateSequence" -> Some expr
        | _ -> None

    let (|AS|_|) (expr : Expr) =

        let (|Functions|_|) name (expr : Expr) =
            match expr with
            | Call (None, mi, [ Var v ]) when mi.Name = name ->
                let f (stmBuilder : StatementBuilder) =
                    stmBuilder.AddStatement name
                    stmBuilder.AddStatement "("
                    stmBuilder.AddStatement v.Name
                    stmBuilder.AddStatement ")"
                Some f
            | Call (None, mi, [ PropertyGet (Some (Var v), pi, []) ]) when mi.Name = name ->
                let f (stmBuilder : StatementBuilder) =
                    stmBuilder.AddStatement name
                    stmBuilder.AddStatement "("
                    stmBuilder.AddStatement v.Name
                    stmBuilder.AddStatement "."
                    stmBuilder.AddStatement pi.Name
                    stmBuilder.AddStatement ")"
                Some f
            | _ -> None

        let functionMatcher (expr : Expr) =
            match expr with
            | Functions "count" fStatement -> fStatement
            | Functions "collect" fStatement -> fStatement
            | _ -> invalidOp (sprintf "AS, unmatched function: %A" expr)

        match expr with
        | Call (Some expr, mi, [ Var v ]) 
            when mi.Name = "AS"
            && mi.DeclaringType.IsGenericType
            && mi.DeclaringType.GetGenericTypeDefinition() = typedefof<AS<_>> -> 
                let fStatement stmBuilder =
                    functionMatcher expr stmBuilder
                    stmBuilder.AddStatement " AS "
                    stmBuilder.AddStatement v.Name

                Some (v, fStatement)
        | _ -> None

module private BasicClause =

    let make (stepState : StepBuilder) clause (expr : Expr) =

        let stmBuilder = StatementBuilder(clause, stepState)

        let extractStatement (exp : Expr) =
            match exp with
            | Value (o, _) -> stmBuilder.AddCoreType o
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
        stepState.Add stmBuilder.Build

module private UnwindClause =
    
    open Helpers

    let make (stepState : StepBuilder) clause (expr : Expr) =

        let stmBuilder = StatementBuilder(clause, stepState)

        let rec inner expr =
            match expr with
            | Let (_, _, expr) -> inner expr
            | TupleGet (expr, _) -> inner expr
            | Lambda (_, expr) -> inner expr
            | AS (_, fStatement) -> fStatement stmBuilder
            | _ -> sprintf "UNWIND CLAUSE: Unrecognized expression: %A" expr |> invalidOp

        inner expr
        stepState.Add stmBuilder.Build

module private MatchClause =

    open Helpers

    let make (stepState : StepBuilder) (clause : Clause) (varDic : VarDic) expr =

        let stmBuilder = StatementBuilder(clause, stepState)

        // Use these since match statements happy with typeof<_>
        let typedefofNode = typedefof<Node<_>>
        let typedefofRel = typedefof<Rel<_>>
        let typedefofIFSNode = typedefof<IFSNode<_>>
        let typedefofIFSRelationship = typedefof<IFSRel<_>>
        let typeofNodeLabel = typeof<NodeLabel>
        let typeofNodeLabelList = typeof<NodeLabel list>
        let typeofRelLabel = typeof<RelLabel>
        let typeofUInt32 = typeof<uint32>
        let typeofUInt32List = typeof<uint32 list>

        let makeIFS (expr : Expr) =

            let makeRecordOrClass typ (exprs : Expr list) =

                let getValues expr =
                    match expr with
                    | Var v -> Choice1Of3 varDic.[v.Name]
                    | PropertyGet (None, _, _) -> Choice1Of3 expr
                    | NewUnionCase (ui, _) when Deserialization.isOption ui.DeclaringType -> Choice1Of3 expr
                    | Value (o, _) -> Choice2Of3 o
                    | PropertyGet (Some _, _, _) -> Choice3Of3 ()
                    | _  -> invalidOp(sprintf "Unmatched Expr when getting field value: %A" expr)

                let fieldValues = List.map getValues exprs

                let mutable isFirst = true

                let build i (pi : PropertyInfo) =
                    let name () =
                        if isFirst then isFirst <- false else stmBuilder.AddStatement ", "
                        stmBuilder.AddStatement pi.Name
                        stmBuilder.AddStatement ": "

                    match fieldValues.[i] with
                    | Choice1Of3 expr ->
                        name ()
                        stmBuilder.AddCoreType expr
                    | Choice2Of3 o ->
                        name ()
                        stmBuilder.AddCoreType o
                    | Choice3Of3 _ -> ()

                stmBuilder.AddStatement "{"
                Deserialization.getProperties typ |> Array.iteri build
                stmBuilder.AddStatement "}"

            let rec inner (expr : Expr) =
                match expr with
                | Coerce (expr, _) -> inner expr
                | Let (_, _, expr) -> inner expr
                | PropertyGet (None, ifs, []) -> stmBuilder.AddStatement ifs.Name
                | Call(None, mi, []) -> stmBuilder.AddStatement mi.Name
                | Var ifs -> stmBuilder.AddStatement ifs.Name
                | ValueWithName (_, _, name) -> stmBuilder.AddStatement name
                | NewRecord (typ, exprs) -> makeRecordOrClass typ exprs
                | NewObject (_, _) -> invalidOp "Classes are not yet supported" // Hard to support classes..
                | _ -> sprintf "Could not build IFS from expression %A" expr |> invalidOp

            inner expr

        let makePathHops (expr : Expr) =

            let makeStatement startValue endValue =
                if endValue = UInt32.MaxValue then sprintf "*%i.." startValue
                else sprintf "*%i..%i" startValue endValue

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
            | ListCons statement -> statement
            | IsCreateSeq (GetRange varDic (startV , endV)) -> makeStatement startV endV
            | _ -> sprintf "Could not build Path Hops from expression %A" expr |> invalidOp
            |> stmBuilder.AddStatement

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

        let makeRelLabel (expr : Expr) =
            let rec inner (expr : Expr) =
                match expr with
                | SpecificCall <@@ (/) @@> (_, _, xs) -> List.sumBy inner xs
                | _ -> extractObject varDic expr :?> RelLabel

            inner expr
            |> string
            |> stmBuilder.AddStatement

        let makeRel (ctrTypes : Type []) (ctrExpr : Expr list) =
            stmBuilder.AddStatement "["
            match ctrTypes, ctrExpr with
            | NoParams -> ()
            | SingleParam [| typedefofIFSRelationship |] param -> makeIFS param
            | SingleParam [| typeofRelLabel |] param  -> makeRelLabel param
            | SingleParam [| typeofUInt32 |] param
            | SingleParam [| typeofUInt32List |] param -> makePathHops param
            | TwoParams [| typedefofIFSRelationship; typeofRelLabel |] (param1, param2) ->
                makeIFS param1
                makeRelLabel param2
            | TwoParams [| typeofRelLabel; typeofUInt32 |] (param1, param2) ->
                makeRelLabel param1
                makePathHops param2
            | TwoParams [| typeofRelLabel; typeofUInt32List |] (param1, param2) ->
                makeRelLabel param1
                makePathHops param2
            | ThreeParams [| typedefofIFSRelationship; typeofRelLabel; typedefofIFSRelationship |] (param1, param2, param3) ->
                makeIFS param1
                makeRelLabel param2
                stmBuilder.AddStatement " "
                makeIFS param3
            | _ -> sprintf "Unexpected Rel constructor: %A" ctrTypes |> invalidOp

            stmBuilder.AddStatement "]"

        let makeNodeLabelList (expr : Expr) =
            match expr with
            | NewUnionCase (ui, _) when ui.Name = "Cons" -> QuotationEvaluator.EvaluateUntyped expr :?> NodeLabel list
            | _ -> extractObject varDic expr :?> NodeLabel list
            |> List.iter (string >> stmBuilder.AddStatement)

        let makeNodeLabel expr =
            extractObject varDic expr
            :?> NodeLabel
            |> string
            |> stmBuilder.AddStatement

        let makeNode (ctrTypes : Type []) (ctrExpr : Expr list) =
            stmBuilder.AddStatement "("
            match ctrTypes, ctrExpr with
            | NoParams -> ()
            | SingleParam [| typeofNodeLabel |] param -> makeNodeLabel param
            | SingleParam [| typeofNodeLabelList |] param -> makeNodeLabelList param
            | SingleParam [| typedefofIFSNode |] param -> makeIFS param
            | TwoParams [| typedefofIFSNode; typeofNodeLabel |] (param1, param2) ->
                makeIFS param1
                makeNodeLabel param2
            | TwoParams [| typedefofIFSNode; typeofNodeLabelList |] (param1, param2) ->
                makeIFS param1
                makeNodeLabelList param2
            | TwoParams [| typedefofIFSNode; typedefofIFSNode |] (param1, param2) ->
                makeIFS param1
                stmBuilder.AddStatement " "
                makeIFS param2
            | ThreeParams [| typedefofIFSNode; typeofNodeLabel; typedefofIFSNode |] (param1, param2, param3) ->
                makeIFS param1
                makeNodeLabel param2
                stmBuilder.AddStatement " "
                makeIFS param3
            | ThreeParams [| typedefofIFSNode; typeofNodeLabelList; typedefofIFSNode |] (param1, param2, param3) ->
                makeIFS param1
                makeNodeLabelList param2
                stmBuilder.AddStatement " "
                makeIFS param3
            | _ -> sprintf "Unexpected Node constructor: %A" ctrTypes |> invalidOp

            stmBuilder.AddStatement ")"

        let (|GetConstructors|_|) fResult (typ : Type) (expr : Expr) =
            let isTyp (ci : ConstructorInfo) =
                if ci.DeclaringType.IsGenericType
                then ci.DeclaringType.GetGenericTypeDefinition() = typ
                else ci.DeclaringType = typ

            let getParamType (pi : ParameterInfo) =
                if pi.ParameterType.IsGenericType && (pi.ParameterType.GetGenericTypeDefinition() = typedefofIFSNode || pi.ParameterType.GetGenericTypeDefinition() = typedefofIFSRelationship)
                then pi.ParameterType.GetGenericTypeDefinition()
                else pi.ParameterType

            match expr with
            | NewObject (ci, paramsExpr) when isTyp ci ->
                let ctTypes =
                    ci.GetParameters()
                    |> Array.map getParamType
                Some (fResult ctTypes paramsExpr)
            | _ -> None

        let (|BuildJoin|_|) (operator : Operators) fResult expr =
            match expr with
            | Call (_, mi, [ left; right ]) when mi.Name = operator.Name ->
                fResult left
                stmBuilder.AddStatement (string operator)
                fResult right
                Some ()
            | _ -> None

        let rec inner (expr : Expr) =
            match expr with
            | Coerce (expr, _)
            | Let (_, _, expr)
            | TupleGet (expr, _)
            | Lambda (_, expr) -> inner expr
            | GetConstructors makeNode typedefofNode rtn
            | GetConstructors makeRel typedefofRel rtn
            | BuildJoin OpMMMM inner rtn
            | BuildJoin OpLMMMM inner rtn
            | BuildJoin OpMMMMG inner rtn
            | BuildJoin OpMM inner rtn
            | BuildJoin OpLMM inner rtn
            | BuildJoin OpMMG inner rtn -> rtn
            | Var v -> invalidOp (sprintf "You must call Node(..) or Rel(..) for Variable %s within the MATCH statement" v.Name)
            | _ -> invalidOp (sprintf "Unable to build MATCH statement from expression: %A" expr)

        inner expr
        stepState.Add stmBuilder.Build

module private WhereSetClause =
    // TODO : full setting of node / rel
    let make (stepState : StepBuilder) clause (expr : Expr) =

        let stmBuilder = StatementBuilder(clause, stepState)

        let buildState fExpr left symbol right =
            fExpr left
            stmBuilder.AddStatement " "
            stmBuilder.AddStatement symbol
            stmBuilder.AddStatement " "
            fExpr right

        let (|Operator|_|) (operator : Operators) fExpr expr =
            match expr with
            | Call (_, mi, [ left; right ]) when mi.Name = operator.Name ->
                Some (buildState fExpr left (string operator) right)
            | _ -> None

        let rec inner (expr : Expr) =
            match expr with
            | Let (_, _, expr)
            | Lambda (_, expr) -> inner expr
            | Operator OpEqual inner finalState
            | Operator OpLess inner finalState
            | Operator OpLessOrEqual inner finalState
            | Operator OpGreater inner finalState
            | Operator OpGreaterOrEqual inner finalState
            | Operator OpNotEqual inner finalState -> finalState
            | IfThenElse (left, right, Value(_, _)) -> buildState inner left "AND" right
            | IfThenElse (left, Value(_, _), right) -> buildState inner left "OR" right
            | NewTuple exprs ->
                exprs
                |> List.iteri (fun i expr ->
                    if i <> 0 then stmBuilder.AddStatement ", "
                    inner expr)
            | NewUnionCase (_, [ singleCase ]) -> inner singleCase
            | NewUnionCase (ui, []) when ui.Name = "None" -> stmBuilder.AddCoreType expr
            | NewUnionCase (ui, _) when ui.Name = "Cons" || ui.Name = "Empty" -> stmBuilder.AddCoreType expr
            | NewArray (_, _) -> stmBuilder.AddCoreType expr
            | Value (_, typ) ->
                //if Deserialization.isIFS typ
                //then stmBuilder.AddSerializedType expr
                //else stmBuilder.AddPrimativeType expr
                stmBuilder.AddCoreType expr
            | PropertyGet (Some (PropertyGet (Some e, pi, _)), _, _) ->
                inner e
                stmBuilder.AddStatement "."
                stmBuilder.AddStatement pi.Name
            | PropertyGet (Some e, pi, _) ->
                inner e
                stmBuilder.AddStatement "."
                stmBuilder.AddStatement pi.Name
            | PropertyGet (None, pi, _) ->
                //if Deserialization.isIFS pi.PropertyType
                //then stmBuilder.AddSerializedType expr
                //else stmBuilder.AddPrimativeType expr
                stmBuilder.AddCoreType expr
            | Var v -> stmBuilder.AddStatement v.Name
            | _ ->
                sprintf "Un matched in WHERE/SET statement: %A" expr
                |> invalidOp

        inner expr
        stepState.Add stmBuilder.Build

module private ReturnClause =

    open Helpers

    let make<'T> (stepState : StepBuilder) clause (expr : Expr) =

        let stmBuilder = StatementBuilder(clause, stepState)

        let maker (expr : Expr) =
            match expr with
            | Value (o, typ) -> 
                let key = StatementBuilder.KeySymbol + stmBuilder.AddCoreTypeRtnKey o
                key, typ
            | Var v -> 
                stmBuilder.AddStatement v.Name
                v.Name, v.Type
            | PropertyGet (Some (Var v), pi, _) -> 
                stmBuilder.AddStatement v.Name
                stmBuilder.AddStatement "."
                stmBuilder.AddStatement pi.Name
                v.Name + "." + pi.Name, pi.PropertyType
            | PropertyGet (None, pi, []) -> 
                stmBuilder.AddStatement pi.Name
                pi.Name, pi.PropertyType
            | AS (v, fStatement) -> 
                fStatement stmBuilder
                v.Name, v.Type.GenericTypeArguments.[0]
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
            | PropertyGet _
            | AS (_, _) -> maker expr |> Choice1Of2
            | NewTuple exprs ->
                exprs
                |> List.mapi (fun i expr ->
                    if i <> 0 then stmBuilder.AddStatement ", "
                    maker expr)
                |> Choice2Of2
            | _ -> sprintf "RETURN. Unrecognized expression: %A" expr |> invalidOp

        let result = inner expr // Must run, otherwise never builds RETURN
        let continuation (di : Generic.IReadOnlyDictionary<string,obj>) = 
            match result with
            | Choice1Of2 keyTyp -> Deserialization.deserialize di keyTyp
            | Choice2Of2 keyTyps ->
                keyTyps
                |> List.map (Deserialization.deserialize di)
                |> Expr.NewTuple
            |> Expr.Cast<'T> 
            |> QuotationEvaluator.Evaluate

        stepState.Add stmBuilder.Build, continuation

[<AutoOpen>]
module CypherBuilder =

    // Initial help came from this great article by Thomas Petricek
    // http://tomasp.net/blog/2015/query-translation/
    // Other helpful articles
    // https://stackoverflow.com/questions/23122639/how-do-i-write-a-computation-expression-builder-that-accumulates-a-value-and-als
    // https://stackoverflow.com/questions/14110532/extended-computation-expressions-without-for-in-do

    // FOR EACH Supports the following commands : CREATE, MERGE, DELETE, FOREACH
    type ForEach () =    
        
        [<CustomOperation(ClauseNames.CREATE, MaintainsVariableSpace = true)>]
        member _.CREATE (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> Node<'N>) : Query<'T,'Result> = NA
        
        [<CustomOperation(ClauseNames.DELETE, MaintainsVariableSpace = true)>]
        member _.DELETE (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Delete) : Query<'T,'Result> = NA
        
        [<CustomOperation(ClauseNames.DETACH_DELETE, MaintainsVariableSpace = true)>]
        member _.DETACH_DELETE (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Delete) : Query<'T,'Result> = NA
        
        [<CustomOperation(ClauseNames.FOREACH, MaintainsVariableSpace = true)>]
        member _.FOREACH (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> ForEach) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.MERGE, MaintainsVariableSpace = true)>]
        member _.MERGE (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> Node<'N>) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.ON_CREATE_SET, MaintainsVariableSpace = true)>]
        member _.ON_CREATE_SET (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Set) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.ON_MATCH_SET, MaintainsVariableSpace = true)>]
        member _.ON_MATCH_SET (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Set) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.SET, MaintainsVariableSpace = true)>]
        member _.SET (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Set) : Query<'T,'Result> = NA

        member _.Yield (source : 'T) : Query<'T,unit> = NA
        
        member _.Zero () : Query<'T,'Result> = NA
        
        member _.For (source : Node<'T>, f : 'T -> Query<'T2, unit>) : Query<'T2, unit> = NA
        
        member _.For (source : Rel<'T>, f : 'T -> Query<'T2, unit>) : Query<'T2, unit> = NA
        
        member this.Run x : unit = ()

    let FOREACH = ForEach()

    type CypherBuilder () =
        
        [<CustomOperation(ClauseNames.ASC, MaintainsVariableSpace = true)>]
        member _.ASC (source : Query<'T,'Result>) : Query<'T,'Result> = NA
        
        [<CustomOperation(ClauseNames.CREATE, MaintainsVariableSpace = true)>]
        member _.CREATE (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> Node<'N>) : Query<'T,'Result> = NA
        
        [<CustomOperation(ClauseNames.DELETE, MaintainsVariableSpace = true)>]
        member _.DELETE (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Delete) : Query<'T,'Result> = NA
        
        [<CustomOperation(ClauseNames.DESC, MaintainsVariableSpace = true)>]
        member _.DESC (source : Query<'T,'Result>) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.DETACH_DELETE, MaintainsVariableSpace = true)>]
        member _.DETACH_DELETE (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Delete) : Query<'T,'Result> = NA
        
        [<CustomOperation(ClauseNames.LIMIT, MaintainsVariableSpace = true)>]
        member _.LIMIT (source : Query<'T,'Result>, count : int64) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.MATCH, MaintainsVariableSpace = true)>]
        member _.MATCH (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> Node<'N>) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.MERGE, MaintainsVariableSpace = true)>]
        member _.MERGE (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> Node<'N>) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.ON_CREATE_SET, MaintainsVariableSpace = true)>]
        member _.ON_CREATE_SET (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Set) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.ON_MATCH_SET, MaintainsVariableSpace = true)>]
        member _.ON_MATCH_SET (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Set) : Query<'T,'Result> = NA

        // TODO: Look at how to handle the possiblitly of getting null into a result set
        // or passing option types into the Node<'T>
        /// Note this can return null
        [<CustomOperation(ClauseNames.OPTIONAL_MATCH, MaintainsVariableSpace = true)>]
        member _.OPTIONAL_MATCH (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> Node<'N>) : Query<'T,'Result> = NA

        // TODO: Can't get the intellisense here by adding in the types as it causes some issues
        [<CustomOperation(ClauseNames.ORDER_BY, MaintainsVariableSpace = true)>]
        member _.ORDER_BY (source : Query<'T,'Result>, [<ProjectionParameter>] f : 'T -> 'Key) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.RETURN, MaintainsVariableSpace = true)>]
        member _.RETURN (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'FinalResult) : Query<'T,'FinalResult> = NA

        [<CustomOperation(ClauseNames.RETURN_DISTINCT, MaintainsVariableSpace = true)>]
        member _.RETURN_DISTINCT (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'FinalResult) : Query<'T,'FinalResult> = NA

        [<CustomOperation(ClauseNames.SET, MaintainsVariableSpace = true)>]
        member _.SET (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Set) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.SKIP, MaintainsVariableSpace = true)>]
        member _.SKIP (source : Query<'T,'Result>, count : int64) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.UNION, MaintainsVariableSpace = true)>]
        member _.UNION (source : Query<'T,'Result>) : Query<'T,'Result> = NA
        
        [<CustomOperation(ClauseNames.UNION_ALL, MaintainsVariableSpace = true)>]
        member _.UNION_ALL (source : Query<'T,'Result>) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.UNWIND, MaintainsVariableSpace = true)>]
        member _.UNWIND (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'AS list) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.WHERE, MaintainsVariableSpace = true)>]
        member _.WHERE (source : Query<'T,'Result>, [<ProjectionParameter>] predicate : 'T -> bool) : Query<'T,'Result> = NA
        
        [<CustomOperation(ClauseNames.WITH, MaintainsVariableSpace = true)>]
        member _.WITH (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'With) : Query<'T,'Result> = NA

        member _.Yield (source : 'T) : Query<'T,unit> = NA
        
        member _.Zero () : Query<'T,unit> = NA
        
        member _.For (source : Query<'T,'Result>, body : 'T -> Query<'T2, unit>) : Query<'T2, unit> = NA
        
        member _.For (source : Node<'T>, body : 'T -> Query<'T2, unit>) : Query<'T2, unit> = NA
        
        member _.For (source : Rel<'T>, body : 'T -> Query<'T2, unit>) : Query<'T2, unit> = NA

        member _.Quote (query : Expr<Query<'T,'Result>>) = NA
        
        member this.Run (expr : Expr<Query<'T,'Result>>) =
            // TODO: This is a bit rough and ready
            let varDic =
                let mutable varExp = None
                let varDic = Generic.Dictionary<string,Expr>()
                let rec inner expr =
                    match expr with
                    | Call (_, mi, [ varValue; yieldEnd ]) when mi.Name = "For" ->
                        varExp <- Some varValue
                        inner yieldEnd
                    | SpecificCall <@@ this.Yield @@> _ -> ()
                    | Let (v, e1, e2) ->
                        if not(varDic.ContainsKey v.Name) then
                            varDic.Add(v.Name, if varExp.IsSome then varExp.Value else e1)
                            varExp <- None
                        inner e2
                    | ShapeCombination (_, exprs) -> List.iter inner exprs
                    | ShapeLambda (_, expr) -> inner expr
                    | ShapeVar _ -> ()
                inner expr.Raw
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
                    let state = WhereSetClause.make state clause thisStep
                    Some (fExpr state stepAbove)
                | _ -> None

            let mutable returnStatement : ReturnContination<'Result> option = None

            let (|Return|_|) (callExpr : Expr) (clause : Clause) (state : StepBuilder) fExpr (expr : Expr) =
                match expr with
                | SpecificCall callExpr (_, _, [ stepAbove; thisStep ]) ->
                    let (state, continuation) = ReturnClause.make<'Result> state clause thisStep
                    returnStatement <- Some continuation
                    Some (fExpr state stepAbove)
                | _ -> None

            let (|Basic|_|) (callExpr : Expr) (clause : Clause) (state : StepBuilder) fExpr (expr : Expr) =
                match expr with
                | SpecificCall callExpr (_, _, [ stepAbove; thisStep ]) ->
                    let state = BasicClause.make state clause thisStep
                    Some (fExpr state stepAbove)
                | _ -> None

            let (|Unwind|_|) (callExpr : Expr) (clause : Clause) (state : StepBuilder) fExpr (expr : Expr) =
                match expr with
                | SpecificCall callExpr (_, _, [ stepAbove; thisStep ]) ->
                    let state = UnwindClause.make state clause thisStep
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
                    | Sequential (Application (expr , _) , _) -> inner state expr
                    | Lambda (var, expr) when var.Type = typeof<ForEach> -> invalidOp "FOREACH is not ready"
                    | SpecificCall <@@ this.Yield @@> _ -> state
                    | Call (_, mi, exprs) when mi.Name = "For" -> state
                    | Let (_, _, expr)
                    | Lambda (_, expr) -> inner state expr
                    | NoStatement <@@ this.ASC @@> ASC state inner stepList
                    | MatchCreateMerge <@@ this.CREATE @@> CREATE state inner stepList
                    | Basic <@@ this.DELETE @@> DELETE state inner stepList
                    | NoStatement <@@ this.DESC @@> DESC state inner stepList
                    | Basic <@@ this.DETACH_DELETE @@> DETACH_DELETE state inner stepList
                    | Basic <@@ this.LIMIT @@> LIMIT state inner stepList
                    | MatchCreateMerge <@@ this.MATCH @@> MATCH state inner stepList
                    | MatchCreateMerge <@@ this.MERGE @@> MERGE state inner stepList
                    | WhereSet <@@ this.ON_CREATE_SET @@> ON_CREATE_SET state inner stepList
                    | WhereSet <@@ this.ON_MATCH_SET @@> ON_MATCH_SET state inner stepList
                    | MatchCreateMerge <@@ this.OPTIONAL_MATCH @@> OPTIONAL_MATCH state inner stepList
                    | Basic <@@ this.ORDER_BY @@> ORDER_BY state inner stepList
                    | Return <@@ this.RETURN @@> RETURN state inner stepList
                    | Return <@@ this.RETURN_DISTINCT @@> RETURN_DISTINCT state inner stepList
                    | WhereSet <@@ this.SET @@> SET state inner stepList
                    | Basic <@@ this.SKIP @@> SKIP state inner stepList
                    | NoStatement <@@ this.UNION @@> UNION state inner stepList
                    | NoStatement <@@ this.UNION_ALL @@> UNION_ALL state inner stepList
                    | Unwind <@@ this.UNWIND @@> UNWIND state inner stepList
                    | WhereSet <@@ this.WHERE @@> WHERE state inner stepList
                    | Basic <@@ this.WITH @@> WITH state inner stepList -> stepList
                    | _ -> sprintf "Un matched method when building Query: %A" expr |> invalidOp

                inner StepBuilder.Init expr

            let stepBuilder = buildQry expr.Raw

            stepBuilder.Build returnStatement

    let cypher = CypherBuilder()