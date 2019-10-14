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

[<NoComparison; NoEquality>]
type Query<'T,'Result> =
    private 
    | NA

type private StepBuilder private (stepNo : int, steps : CypherStep list) =
    member _.StepNo = stepNo
    member _.Steps = steps
    member this.Add (step : CypherStep) = StepBuilder(this.StepNo + 1, step :: this.Steps)
    static member Init = StepBuilder(1, [])

type private StatementBuilder(clause: Clause, stepBuilder : StepBuilder) =
    let mutable prms : ParameterList = []
    let mutable prmCount : int = 1
    let stepCount = stepBuilder.StepNo
    let parameterizedSb = Text.StringBuilder()
    let nonParameterizedSb = Text.StringBuilder()
    let addParamterized (str : string) = parameterizedSb.Append str |> ignore
    let addNonParamterized (str : string) = nonParameterizedSb.Append str |> ignore

    static member KeySymbol = "$"
    static member FixStringParameter (o : obj) =
        match o with
        | :? unit -> "null"
        | :? string as s ->  sprintf "\"%s\"" s
        | :? bool as b -> b.ToString().ToLower()
        | :? Generic.List<obj> as xs ->
            xs
            |> Seq.map string
            |> String.concat ", "
            |> sprintf "[%s]"

        | :? Generic.Dictionary<string, obj> as d ->
            d
            |> Seq.map (fun kv -> kv.Key + ": " + StatementBuilder.FixStringParameter kv.Value)
            |> String.concat ", "
            |> sprintf "{%s}"
        | _ -> string o

    member private this.Clause = clause

    member this.GenerateKey() =
        let key = sprintf "step%iparam%i" stepCount prmCount
        prmCount <- prmCount + 1
        addParamterized StatementBuilder.KeySymbol
        addParamterized key
        key

    member private this.Add (o : obj option) =
        let key = this.GenerateKey()
        match o with
        | Some o -> addNonParamterized (StatementBuilder.FixStringParameter o)
        | None -> addNonParamterized "null"
        prms <- (key, o) :: prms

    member this.AddObj (o : obj) =
        Serialization.fixTypes o
        |> this.Add

    member this.AddPrimativeType expr =
        QuotationEvaluator.EvaluateUntyped expr
        |> Serialization.fixTypes
        |> this.Add

    member this.AddSerializedType expr =
        QuotationEvaluator.EvaluateUntyped expr
        //:?> IFSEntity
        |> Serialization.serialize
        |> box
        |> Some
        |> this.Add

    member _.AddStatement (stmt : string) =
        addNonParamterized stmt
        addParamterized stmt

    member this.Build = CypherStep(this.Clause, string parameterizedSb, string nonParameterizedSb, prms)

module private MatchClause =

    type private Operators =
        | OpMMMM
        | OpLMMMM
        | OpMMMMG
        | OpMM
        | OpLMM
        | OpMMG
        member this.Name =
            match this with
            | OpMMMM -> "op_MinusMinusMinusMinus"
            | OpLMMMM -> "op_LessMinusMinusMinusMinus"
            | OpMMMMG -> "op_MinusMinusMinusMinusGreater"
            | OpMM -> "op_MinusMinus"
            | OpLMM -> "op_LessMinusMinus"
            | OpMMG -> "op_MinusMinusGreater"
        override this.ToString() =
            match this with
            | OpMMMM -> "--"
            | OpLMMMM -> "<--"
            | OpMMMMG -> "-->"
            | OpMM -> "-"
            | OpLMM -> "<-"
            | OpMMG -> "->"

    let make (stepState : StepBuilder) (clause : Clause) (varDic : VarDic) expr =

        let stmBuilder = StatementBuilder(clause, stepState)

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

        let extractObject (expr : Expr) =
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
                        // In this case the obj is actually Node<'N> or Rel<'R>
                        // so will need to create an instance of it and call the static member
                        if Node.IsTypeDefOf varObj || Rel.IsTypeDefOf varObj
                        then
                            Deserialization.createNullRecordOrClass var.Type
                            |> pi.GetValue
                        else varObj

                inner expr

            | _ -> sprintf "Could not build Label from expression %A" expr |> invalidOp

        let makeIFS (expr : Expr) =

            let makeRecordOrClass typ (exprs : Expr list) =

                let getValues expr =
                    match expr with
                    | Var v -> Choice1Of3 varDic.[v.Name]
                    | Value (o, _) -> Choice2Of3 o
                    | PropertyGet (Some _, _, _) -> Choice3Of3 ()
                    | PropertyGet (None, _, _) -> Choice1Of3 expr
                    | NewUnionCase (ui, _) when Deserialization.isOption ui.DeclaringType -> Choice1Of3 expr
                    | _  -> invalidOp(sprintf "Unmatched Expr when getting field value: %A" expr)

                let fieldValues = List.map getValues exprs

                let build i (pi : PropertyInfo) =
                    let name () =
                        if i <> 0 then stmBuilder.AddStatement ", "
                        stmBuilder.AddStatement pi.Name
                        stmBuilder.AddStatement ": "

                    match fieldValues.[i] with
                    | Choice1Of3 expr ->
                        name ()
                        stmBuilder.AddPrimativeType expr
                    | Choice2Of3 o ->
                        name ()
                        stmBuilder.AddObj o
                    | Choice3Of3 _ -> ()

                stmBuilder.AddStatement "{"

                Deserialization.getProperties typ
                |> Array.iteri build

                stmBuilder.AddStatement "}"

            let rec inner (expr : Expr) =
                match expr with
                | Coerce (expr, _) -> inner expr
                | Let (_, _, expr) -> inner expr
                | PropertyGet (None, ifs, []) -> stmBuilder.AddStatement ifs.Name
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

            let (|GetRange|_|) (expr : Expr) =
                match expr with
                | SpecificCall <@ (..) @> (None, _, [ expr1; expr2 ]) ->
                    let startValue = extractObject expr1 :?> uint32
                    let endValue = extractObject expr2 :?> uint32
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
                | PropertyGet (_, pi, _) when pi.PropertyType = typeofUInt32 -> extractObject expr |> Some
                | _ -> None

            let (|IntList|_|) (expr : Expr) =
                match expr with
                | Var var when var.Type = typeofUInt32List -> makeListFromExpr varDic.[var.Name] |> Some
                | Value (_, t) when t = typeofUInt32List -> makeListFromExpr expr |> Some
                | PropertyGet (_, pi, _) when pi.PropertyType = typeofUInt32List ->
                    extractObject expr
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
                | SpecificCall <@ (/) @> (_, _, xs) -> List.sumBy inner xs
                | _ -> extractObject expr :?> RelLabel

            inner expr
            |> string
            |> stmBuilder.AddStatement

        let makeRel (ctrTypes : Type []) (ctrExpr : Expr list) =
            stmBuilder.AddStatement "["
            match ctrTypes, ctrExpr with
            | NoParams -> ()
            | SingleParam [| typeofIFSRelationship |] param -> makeIFS param
            | SingleParam [| typeofRelLabel |] param  -> makeRelLabel param
            | SingleParam [| typeofUInt32 |] param
            | SingleParam [| typeofUInt32List |] param -> makePathHops param
            | TwoParams [| typeofIFSRelationship; typeofRelLabel |] (param1, param2) ->
                makeIFS param1
                makeRelLabel param2
            | TwoParams [| typeofRelLabel; typeofUInt32 |] (param1, param2) ->
                makeRelLabel param1
                makePathHops param2
            | TwoParams [| typeofRelLabel; typeofUInt32List |] (param1, param2) ->
                makeRelLabel param1
                makePathHops param2
            | ThreeParams [| typeofIFSRelationship; typeofRelLabel; typeofIFSRelationship |] (param1, param2, param3) ->
                makeIFS param1
                makeRelLabel param2
                stmBuilder.AddStatement " "
                makeIFS param3
            | _ -> sprintf "Unexpected Rel constructor: %A" ctrTypes |> invalidOp

            stmBuilder.AddStatement "]"

        let makeNodeLabelList (expr : Expr) =
            match expr with
            | NewUnionCase (ui, _) when ui.Name = "Cons" -> QuotationEvaluator.EvaluateUntyped expr :?> NodeLabel list
            | _ -> extractObject expr :?> NodeLabel list
            |> List.iter (string >> stmBuilder.AddStatement)

        let makeNodeLabel expr =
            extractObject expr
            :?> NodeLabel
            |> string
            |> stmBuilder.AddStatement

        let makeNode (ctrTypes : Type []) (ctrExpr : Expr list) =
            stmBuilder.AddStatement "("
            match ctrTypes, ctrExpr with
            | NoParams -> ()
            | SingleParam [| typeofNodeLabel |] param -> makeNodeLabel param
            | SingleParam [| typeofNodeLabelList |] param ->  makeNodeLabelList param
            | SingleParam [| typeofIFSNode |] param -> makeIFS param
            | TwoParams [| typeofIFSNode; typeofNodeLabel |] (param1, param2) ->
                makeIFS param1
                makeNodeLabel param2
            | TwoParams [| typeofIFSNode; typeofNodeLabelList |] (param1, param2) ->
                makeIFS param1
                makeNodeLabelList param2
            | TwoParams [| typeofIFSNode; typeofIFSNode |] (param1, param2) ->
                makeIFS param1
                stmBuilder.AddStatement " "
                makeIFS param2
            | ThreeParams [| typeofIFSNode; typeofNodeLabel; typeofIFSNode |] (param1, param2, param3) ->
                makeIFS param1
                makeNodeLabel param2
                stmBuilder.AddStatement " "
                makeIFS param3
            | ThreeParams [| typeofIFSNode; typeofNodeLabelList; typeofIFSNode |] (param1, param2, param3) ->
                makeIFS param1
                makeNodeLabelList param2
                stmBuilder.AddStatement " "
                makeIFS param3
            | _ -> sprintf "Unexpected Node constructor: %A" ctrTypes |> invalidOp

            stmBuilder.AddStatement ")"

        let (|GetConstructors|_|) fResult (typ : Type) (expr : Expr) =
            match expr with
            | NewObject (ci, paramsExpr) when ci.DeclaringType = typ -> 
                let ctTypes =   
                    ci.GetParameters() 
                    |> Array.map (fun x -> x.ParameterType)
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
            | GetConstructors makeRel typeofRel rtn
            | GetConstructors makeNode typeofNode rtn
            | BuildJoin OpMMMM inner rtn
            | BuildJoin OpLMMMM inner rtn
            | BuildJoin OpMMMMG inner rtn
            | BuildJoin OpMM inner rtn
            | BuildJoin OpLMM inner rtn
            | BuildJoin OpMMG inner rtn -> rtn
            | Var v -> invalidOp (sprintf "You must call Node(..) or Rel(..) for Variable %s within the MATCH statement" v.Name)
            | _ -> invalidOp (sprintf "Unable to build MATCH statement from expression: %A" expr)

        inner expr

        stepState.Add(stmBuilder.Build)

module private WhereAndSetStatement =

    let [<Literal>] private IFSNode = "IFSNode"
    let [<Literal>] private IFSRelationship = "IFSRelationship"

    let hasInterface (typ : Type) (name : string) = typ.GetInterface name |> isNull |> not

    let isIFS (typ : Type) = hasInterface typ IFSNode || hasInterface typ IFSRelationship

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
            | Let (_, _, expr)
            | Lambda (_, expr) -> inner expr
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
                |> List.iteri (fun i expr ->
                    if i <> 0 then stmBuilder.AddStatement ", "
                    inner expr)
            | NewUnionCase (_, [ singleCase ]) -> inner singleCase
            | NewUnionCase (ui, []) when ui.Name = "None" -> stmBuilder.AddPrimativeType expr
            | NewUnionCase (ui, _) when ui.Name = "Cons" || ui.Name = "Empty" -> stmBuilder.AddPrimativeType expr
            | NewArray (_, _) -> stmBuilder.AddPrimativeType expr
            | Value (_, typ) ->
                if isIFS typ
                then stmBuilder.AddSerializedType expr
                else stmBuilder.AddPrimativeType expr
            | PropertyGet (Some (PropertyGet (Some e, pi, _)), _, _) ->
                inner e
                stmBuilder.AddStatement "."
                stmBuilder.AddStatement pi.Name
            | PropertyGet (Some e, pi, _) ->
                inner e
                stmBuilder.AddStatement "."
                stmBuilder.AddStatement pi.Name
            | PropertyGet (None, pi, _) ->
                if isIFS pi.PropertyType
                then stmBuilder.AddSerializedType expr
                else stmBuilder.AddPrimativeType expr
            | Var v -> stmBuilder.AddStatement v.Name
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
                |> sprintf "RETURN. Trying to extract statement but couldn't match expression: %A"
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

        let result di = continuation di |> Expr.Cast<'T> |> QuotationEvaluator.Evaluate

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
            | NoReturnClause -> invalidOp "You must always return something since its F# - use RETURN ()"
            | Continuation c -> c

    type CypherBuilder() =

        member _.Yield (source : 'T) : Query<'T,unit> = NA

        member _.For (source : Node<'T>, f : 'T -> Query<'T2, unit>) : Query<'T2, unit> = NA
        
        member _.For (source : Rel<'T>, f : 'T -> Query<'T2, unit>) : Query<'T2, unit> = NA

        [<CustomOperation(ClauseNames.MATCH, MaintainsVariableSpace = true)>]
        member _.MATCH (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> Node) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.OPTIONAL_MATCH, MaintainsVariableSpace = true)>]
        member _.OPTIONAL_MATCH (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> Node) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.CREATE, MaintainsVariableSpace = true)>]
        member _.CREATE (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> Node) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.MERGE, MaintainsVariableSpace = true)>]
        member _.MERGE (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> Node) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.WHERE, MaintainsVariableSpace = true)>]
        member _.WHERE (source : Query<'T,'Result>, [<ProjectionParameter>] predicate : 'T -> bool) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.SET, MaintainsVariableSpace = true)>]
        member _.SET (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Set) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.RETURN, MaintainsVariableSpace = true)>]
        member _.RETURN (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'FinalResult) : Query<'T,'FinalResult> = NA

        [<CustomOperation(ClauseNames.RETURN_DISTINCT, MaintainsVariableSpace = true)>]
        member _.RETURN_DISTINCT (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'FinalResult) : Query<'T,'FinalResult> = NA

        [<CustomOperation(ClauseNames.DELETE, MaintainsVariableSpace = true)>]
        member _.DELETE (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Delete) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.DETACH_DELETE, MaintainsVariableSpace = true)>]
        member _.DETACH_DELETE (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Delete) : Query<'T,'Result> = NA

        // TODO: Can't get the intellisense here by adding in the types as it causes some issues
        [<CustomOperation(ClauseNames.ORDER_BY, MaintainsVariableSpace = true)>]
        member _.ORDER_BY (source : Query<'T,'Result>, [<ProjectionParameter>] f : 'T -> 'Key) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.DESC, MaintainsVariableSpace = true)>]
        member _.DESC (source : Query<'T,'Result>) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.SKIP, MaintainsVariableSpace = true)>]
        member _.SKIP (source : Query<'T,'Result>, count : int64) : Query<'T,'Result> = NA

        [<CustomOperation(ClauseNames.LIMIT, MaintainsVariableSpace = true)>]
        member _.LIMIT (source : Query<'T,'Result>, count : int64) : Query<'T,'Result> = NA

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
                    | SpecificCall <@ this.Yield @> _ -> ()
                    //| SpecificCall <@ this.For @> (_, _, [ varValue; yieldEnd ]) ->
                    //    varExp <- Some varValue
                    //    inner yieldEnd
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

            let mutable returnStatement : ReturnContination<'Result> = NoReturnClause

            let (|Return|_|) (callExpr : Expr) (clause : Clause) (state : StepBuilder) fExpr (expr : Expr) =
                match expr with
                | SpecificCall callExpr (_, _, [ stepAbove; thisStep ]) ->
                    let (state, continuation) = ReturnClause.make<'Result> state clause thisStep
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
                    | SpecificCall <@ this.Yield @> _ -> state
                    //| SpecificCall <@ this.For @> (_, _, _) -> state
                    | Call (_, mi, [ varValue; yieldEnd ]) when mi.Name = "For" -> state
                    | Let (_, _, expr)
                    | Lambda (_, expr) -> inner state expr
                    | MatchCreateMerge <@ this.MATCH @> MATCH state inner stepList
                    | MatchCreateMerge <@ this.OPTIONAL_MATCH @> OPTIONAL_MATCH state inner stepList
                    | MatchCreateMerge <@ this.CREATE @> CREATE state inner stepList
                    | MatchCreateMerge <@ this.MERGE @> MERGE state inner stepList
                    | WhereSet <@ this.WHERE @> WHERE state inner stepList
                    | WhereSet <@ this.SET @> SET state inner stepList
                    | Return <@ this.RETURN @> RETURN state inner stepList
                    | Return <@ this.RETURN_DISTINCT @> RETURN_DISTINCT state inner stepList
                    | Basic <@ this.DELETE @> DELETE state inner stepList
                    | Basic <@ this.DETACH_DELETE @> DETACH_DELETE state inner stepList
                    | Basic <@ this.ORDER_BY @> ORDER_BY state inner stepList
                    | NoStatement <@ this.DESC @> DESC state inner stepList
                    | Basic <@ this.SKIP @> SKIP state inner stepList
                    | Basic <@ this.LIMIT @> LIMIT state inner stepList
                    | Basic <@ this.DELETE @> DELETE state inner stepList -> stepList
                    | _ -> sprintf "Un matched method when building Query: %A" expr |> invalidOp

                inner StepBuilder.Init expr

            let stepBuilder = buildQry expr

            Cypher(stepBuilder.Steps, returnStatement.Value)

    let cypher = CypherBuilder()