namespace FSharp.Data.Cypher

open System
open System.Reflection
open System.Collections
open FSharp.Quotations
open FSharp.Quotations.Patterns
open FSharp.Quotations.DerivedPatterns
open FSharp.Quotations.ExprShape

type private VarDic = Generic.IReadOnlyDictionary<string,Expr>

[<NoComparison; NoEquality>]
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
        | OpEqual -> nameof ( = )
        | OpLess -> nameof ( < )
        | OpGreater -> nameof ( > )
        | OpNotEqual -> nameof ( <> )
        | OpLessOrEqual -> nameof ( <= )
        | OpGreaterOrEqual -> nameof ( >= )
        | OpMM -> nameof ( -- )
        | OpLMM -> nameof ( <-- )
        | OpMMG -> nameof ( --> )
        | OpMMMM -> nameof ( ---- )
        | OpLMMMM -> nameof ( <---- )
        | OpMMMMG -> nameof ( ----> )
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

[<AbstractClass; Sealed>]
type QuotationEvaluator =
    static member EvaluateUntyped (expr : Expr) = Linq.RuntimeHelpers.LeafExpressionConverter.EvaluateQuotation expr
    static member Evaluate (expr : Expr<'T>) = Linq.RuntimeHelpers.LeafExpressionConverter.EvaluateQuotation expr :?> 'T

[<Sealed; NoComparison; NoEquality>]
type private StepBuilder(serializer : Serializer) =
    let mutable prmCount = 0
    let mutable prms : ParameterList = []
    let mutable steps : CypherStep list = []
    let parameterizedSb = Text.StringBuilder()
    let nonParameterizedSb = Text.StringBuilder()
    let addParamterized (str : string) = parameterizedSb.Append str |> ignore
    let addNonParamterized (str : string) = nonParameterizedSb.Append str |> ignore

    let rec fixStringParameter (o : obj) =
        match o with
        | :? unit -> "null"
        | :? string as s -> "\"" + s.Replace("""\""", """\\""") + "\"" // TODO: See https://neo4j.com/docs/cypher-manual/3.5/syntax/expressions/ for full list
        | :? bool as b -> b.ToString().ToLower()
        | :? ResizeArray<obj> as xs ->
            xs
            |> Seq.map string
            |> String.concat ", "
            |> sprintf "[%s]"

        | :? Generic.Dictionary<string, obj> as d ->
            d
            |> Seq.map (function KeyValue (k, v) -> k + ": " + fixStringParameter v)
            |> String.concat ", "
            |> sprintf "{%s}"
        | _ -> string o

    let add (o : obj option) =
        let key = "p" + prmCount.ToString("x2")
        prmCount <- prmCount + 1
        addParamterized StepBuilder.KeySymbol
        addParamterized key

        match o with
        | Some o -> addNonParamterized (fixStringParameter o)
        | None -> addNonParamterized "null"
        prms <- (key, o) :: prms
        key

    member _.Serialize = serializer

    static member KeySymbol = "$"

    static member JoinTuple action (stmBuilder : StepBuilder) i v =
        if i <> 0 then stmBuilder.AddStatement ", "
        action v

    member _.Build (continuation : ReturnContination<'T> option) =
        Cypher<'T>(continuation, Query(steps))

    member this.AddTypeRtnKey (o : obj) = this.Serialize o |> add

    member this.AddType (o : obj) = this.AddTypeRtnKey o |> ignore

    member this.AddType (expr : Expr) = QuotationEvaluator.EvaluateUntyped expr |> this.AddType

    member _.AddStatement (stmt : string) =
        addNonParamterized stmt
        addParamterized stmt

    member _.FinaliseClause (clause : Clause) =
        let step = CypherStep(clause, string parameterizedSb, string nonParameterizedSb, prms)
        parameterizedSb.Clear() |> ignore
        nonParameterizedSb.Clear() |> ignore
        prms <- []
        steps <- step :: steps

module private Helpers =

    open FSharp.Data.Cypher.Functions
    open Aggregating
    
    [<RequireQualifiedAccess>]
    type LabelTypes =
        | NodeLabel
        | RelLabel
        | NodeLabelList
            
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

    let evalulateVar (varDic : VarDic) (expr : Expr) =
        match expr with
        | NewObject (_, [ _ ]) -> QuotationEvaluator.EvaluateUntyped expr
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
                    if Node.IsTypeDefOf varObj || Rel.IsTypeDefOf varObj then
                        TypeHelpers.createNullRecordOrClass var.Type
                        |> pi.GetValue
                    else varObj

            inner expr

        | _ -> sprintf "Could not evaluate Var from expression %A" expr |> invalidOp

    let (|GetRange|_|) (varDic : VarDic) (expr : Expr) =
        match expr with
        | SpecificCall <@@ (..) @@> (None, _, [ expr1; expr2 ]) ->
            let startValue = evalulateVar varDic expr1 :?> uint32
            let endValue = evalulateVar varDic expr2 :?> uint32
            Some (startValue, endValue)
        | _ -> None

    let (|IsCreateSeq|_|) (expr : Expr) =
        match expr with
        | Call (None, mi, [ Coerce (Call (None, mi2, [ expr ]), _) ])
            when mi.Name = "ToList" && mi2.Name = "CreateSequence" -> Some expr
        | _ -> None

    let makeLabel (stepBuilder : StepBuilder) (varDic : VarDic) (labelType : LabelTypes) (expr : Expr) =
        match labelType with
        | LabelTypes.NodeLabel -> 
            evalulateVar varDic expr
            :?> NodeLabel
            |> string
            |> stepBuilder.AddStatement
        | LabelTypes.RelLabel ->
            let rec inner (expr : Expr) =
                match expr with
                | SpecificCall <@@ (/) @@> (_, _, xs) -> List.sumBy inner xs
                | _ -> evalulateVar varDic expr :?> RelLabel

            inner expr
            |> string
            |> stepBuilder.AddStatement
        | LabelTypes.NodeLabelList ->
            match expr with
            | NewUnionCase (ui, _) when ui.Name = "Cons" -> QuotationEvaluator.EvaluateUntyped expr :?> NodeLabel list
            | _ -> evalulateVar varDic expr :?> NodeLabel list
            |> List.iter (string >> stepBuilder.AddStatement)
    
    let (|IsIFSLabelTuple|_|) (expr : Expr) =
        match expr with
        | NewTuple [ entity; label ] when TypeHelpers.isIFS entity.Type && NodeLabel.IsLabel label.Type ->
            Some (entity, label, LabelTypes.NodeLabel)
        | NewTuple [ entity; label ] when TypeHelpers.isIFS entity.Type && RelLabel.IsLabel label.Type ->
            Some (entity, label, LabelTypes.RelLabel)
        | NewTuple [ entity; NewUnionCase ui ] when (fst ui).Name = "Cons" -> 
            Some (entity, Expr.NewUnionCase ui, LabelTypes.NodeLabelList)
        | _ -> None

    let (|AS|_|) (expr : Expr) =

        let (|Functions|_|) name (expr : Expr) =
            match expr with
            | Call (None, mi, [ Var v ]) when mi.Name = name ->
                let f (stepBuilder : StepBuilder) =
                    stepBuilder.AddStatement name
                    stepBuilder.AddStatement "("
                    stepBuilder.AddStatement v.Name
                    stepBuilder.AddStatement ")"
                Some f
            | Call (None, mi, [ PropertyGet (Some (Var v), pi, []) ]) when mi.Name = name ->
                let f (stepBuild : StepBuilder) =
                    stepBuild.AddStatement name
                    stepBuild.AddStatement "("
                    stepBuild.AddStatement v.Name
                    stepBuild.AddStatement "."
                    stepBuild.AddStatement pi.Name
                    stepBuild.AddStatement ")"
                Some f
            | _ -> None

        let functionMatcher (expr : Expr) =
            match expr with
            | Functions (nameof avg) fStatement
            | Functions (nameof collect) fStatement
            | Functions (nameof count) fStatement
            | Functions (nameof max) fStatement
            | Functions (nameof min) fStatement
            | Functions (nameof sum) fStatement -> fStatement
            | _ -> invalidOp (sprintf "AS, unmatched function: %A" expr)

        match expr with
        | Call (Some expr, mi, [ Var v ]) when mi.Name = "AS" && AS.IsTypeDefOf mi.DeclaringType ->
            let fStatement stepBuilder =
                functionMatcher expr stepBuilder
                stepBuilder.AddStatement " AS "
                stepBuilder.AddStatement v.Name
            Some (v, fStatement)
        | _ -> None

module private BasicClause =

    let make (stepBuilder : StepBuilder) (expr : Expr) =

        let extractStatement (exp : Expr) =
            match exp with
            | Value (o, _) -> stepBuilder.AddType o
            | Var v -> stepBuilder.AddStatement v.Name
            | PropertyGet (Some (Var v), pi, _) ->
                stepBuilder.AddStatement v.Name
                stepBuilder.AddStatement "."
                stepBuilder.AddStatement pi.Name
            | PropertyGet (None, pi, _) -> stepBuilder.AddStatement pi.Name
            | _ ->
                exp
                |> sprintf "Trying to extract statement but couldn't match expression: %A"
                |> invalidOp

        let rec inner expr =
            match expr with
            | Let (_, _, expr) | Lambda (_, expr) -> inner expr
            | Value _ | Var _ | PropertyGet _ -> extractStatement expr
            | Helpers.AS (v, fStatement) -> fStatement stepBuilder
            | NewTuple exprs -> exprs |> List.iteri (StepBuilder.JoinTuple extractStatement stepBuilder)
            | _ -> sprintf "BASIC CLAUSE: Unrecognized expression: %A" expr |> invalidOp

        inner expr

module private RemoveClause =

    open Helpers

    let extractStatement (stepBuilder : StepBuilder) (varDic : VarDic) (entity, label, labelType) =
        match entity with
        | Var v -> stepBuilder.AddStatement v.Name
        | PropertyGet (Some (Var v), pi, _) ->
            stepBuilder.AddStatement v.Name
            stepBuilder.AddStatement "."
            stepBuilder.AddStatement pi.Name
        | PropertyGet (None, pi, _) -> stepBuilder.AddStatement pi.Name
        | _ ->
            entity
            |> sprintf "Label SET/REMOVE: Couldn't extact entity : %A"
            |> invalidOp

        makeLabel stepBuilder varDic labelType label

    let make (stepBuilder : StepBuilder) (varDic : VarDic) (expr : Expr) =
        let rec inner expr =
            match expr with
            | Let (_, _, expr) 
            | Lambda (_, expr) -> inner expr
            | IsIFSLabelTuple res -> extractStatement stepBuilder varDic res
            | NewTuple exprs -> exprs |> List.iteri (StepBuilder.JoinTuple inner stepBuilder)
            | _ -> 
                sprintf "REMOVE statements must be in the form of: REMOVE ((node, Label), (rel, Label), ..). You passed in: %A" expr 
                |> invalidOp

        inner expr

module private UnwindClause =

    open Helpers

    let make (stepBuilder : StepBuilder) (expr : Expr) =

        let rec inner expr =
            match expr with
            | Let (_, _, expr) | TupleGet (expr, _) | Lambda (_, expr) -> inner expr
            | AS (_, fStatement) -> fStatement stepBuilder
            | _ -> sprintf "UNWIND CLAUSE: Unrecognized expression: %A" expr |> invalidOp

        inner expr

module private MatchClause =

    open Helpers
    
    let make (stepBuilder : StepBuilder) (varDic : VarDic) expr =

        let makeIFS (expr : Expr) =

            let makeRecordOrClass typ (exprs : Expr list) =

                let getValues expr =
                    match expr with
                    | Var v -> Choice1Of3 varDic.[v.Name]
                    | PropertyGet (None, _, _) -> Choice1Of3 expr
                    | NewUnionCase (ui, _) when TypeHelpers.isOption ui.DeclaringType -> Choice1Of3 expr
                    | Value (o, _) -> Choice2Of3 o
                    | PropertyGet (Some _, _, _) -> Choice3Of3 ()
                    | _  -> invalidOp(sprintf "Unmatched Expr when getting field value: %A" expr)

                let fieldValues = List.map getValues exprs

                let mutable isFirst = true

                let build i (pi : PropertyInfo) =
                    let name () =
                        if isFirst then isFirst <- false else stepBuilder.AddStatement ", "
                        stepBuilder.AddStatement pi.Name
                        stepBuilder.AddStatement ": "

                    match fieldValues.[i] with
                    | Choice1Of3 expr ->
                        name ()
                        stepBuilder.AddType expr
                    | Choice2Of3 o ->
                        name ()
                        stepBuilder.AddType o
                    | Choice3Of3 _ -> ()

                stepBuilder.AddStatement "{"
                TypeHelpers.getProperties typ |> Array.iteri build
                stepBuilder.AddStatement "}"

            let rec inner (expr : Expr) =
                match expr with
                | Coerce (expr, _) | Let (_, _, expr) -> inner expr
                | PropertyGet (None, ifs, []) -> stepBuilder.AddStatement ifs.Name
                | Call(None, mi, []) -> stepBuilder.AddStatement mi.Name
                | Var ifs -> stepBuilder.AddStatement ifs.Name
                | ValueWithName (_, _, name) -> stepBuilder.AddStatement name
                | NewRecord (typ, exprs) -> makeRecordOrClass typ exprs
                | NewObject (_, _) -> invalidOp "Classes are not yet supported" // Hard to support classes..
                | _ -> sprintf "Could not build IFS from expression %A" expr |> invalidOp

            inner expr

        let makePathHops (expr : Expr) =

            let makeStatement startValue endValue =
                if endValue = UInt32.MaxValue
                then sprintf "*%i.." startValue
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
                | PropertyGet (_, pi, _) when pi.PropertyType = typeofUInt32 -> evalulateVar varDic expr |> Some
                | _ -> None

            let (|IntList|_|) (expr : Expr) =
                match expr with
                | Var var when var.Type = typeofUInt32List -> makeListFromExpr varDic.[var.Name] |> Some
                | Value (_, t) when t = typeofUInt32List -> makeListFromExpr expr |> Some
                | PropertyGet (_, pi, _) when pi.PropertyType = typeofUInt32List ->
                    evalulateVar varDic expr
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
            |> stepBuilder.AddStatement

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

        let makeRel (ctrTypes : Type []) (ctrExpr : Expr list) =
            stepBuilder.AddStatement "["
            match ctrTypes, ctrExpr with
            | NoParams -> ()
            | SingleParam [| typedefofIFSRelationship |] param -> makeIFS param
            | SingleParam [| typeofRelLabel |] param  -> makeLabel stepBuilder varDic LabelTypes.RelLabel param
            | SingleParam [| typeofUInt32 |] param
            | SingleParam [| typeofUInt32List |] param -> makePathHops param
            | TwoParams [| typedefofIFSRelationship; typeofRelLabel |] (param1, param2) ->
                makeIFS param1
                makeLabel stepBuilder varDic LabelTypes.RelLabel param2
            | TwoParams [| typeofRelLabel; typeofUInt32 |] (param1, param2) ->
                makeLabel stepBuilder varDic LabelTypes.RelLabel param1
                makePathHops param2
            | TwoParams [| typeofRelLabel; typeofUInt32List |] (param1, param2) ->
                makeLabel stepBuilder varDic LabelTypes.RelLabel param1
                makePathHops param2
            | ThreeParams [| typedefofIFSRelationship; typeofRelLabel; typedefofIFSRelationship |] (param1, param2, param3) ->
                makeIFS param1
                makeLabel stepBuilder varDic LabelTypes.RelLabel param2
                stepBuilder.AddStatement " "
                makeIFS param3
            | _ -> sprintf "Unexpected Rel constructor: %A" ctrTypes |> invalidOp

            stepBuilder.AddStatement "]"

        let makeNode (ctrTypes : Type []) (ctrExpr : Expr list) =
            stepBuilder.AddStatement "("
            match ctrTypes, ctrExpr with
            | NoParams -> ()
            | SingleParam [| typeofNodeLabel |] param -> makeLabel stepBuilder varDic LabelTypes.NodeLabel param
            | SingleParam [| typeofNodeLabelList |] param -> makeLabel stepBuilder varDic LabelTypes.NodeLabelList param
            | SingleParam [| typedefofIFSNode |] param -> makeIFS param
            | TwoParams [| typedefofIFSNode; typeofNodeLabel |] (param1, param2) ->
                makeIFS param1
                makeLabel stepBuilder varDic LabelTypes.NodeLabel param2
            | TwoParams [| typedefofIFSNode; typeofNodeLabelList |] (param1, param2) ->
                makeIFS param1
                makeLabel stepBuilder varDic LabelTypes.NodeLabelList param2
            | TwoParams [| typedefofIFSNode; typedefofIFSNode |] (param1, param2) ->
                makeIFS param1
                stepBuilder.AddStatement " "
                makeIFS param2
            | ThreeParams [| typedefofIFSNode; typeofNodeLabel; typedefofIFSNode |] (param1, param2, param3) ->
                makeIFS param1
                makeLabel stepBuilder varDic LabelTypes.NodeLabel param2
                stepBuilder.AddStatement " "
                makeIFS param3
            | ThreeParams [| typedefofIFSNode; typeofNodeLabelList; typedefofIFSNode |] (param1, param2, param3) ->
                makeIFS param1
                makeLabel stepBuilder varDic LabelTypes.NodeLabelList param2
                stepBuilder.AddStatement " "
                makeIFS param3
            | _ -> sprintf "Unexpected Node constructor: %A" ctrTypes |> invalidOp

            stepBuilder.AddStatement ")"

        let (|GetConstructors|_|) (typ : Type) (expr : Expr) =
            let isTyp (ci : ConstructorInfo) =
                if ci.DeclaringType.IsGenericType
                then ci.DeclaringType.GetGenericTypeDefinition() = typ
                else ci.DeclaringType = typ

            let getParamType (pi : ParameterInfo) =
                if pi.ParameterType.IsGenericType then
                    let typ = pi.ParameterType.GetGenericTypeDefinition()
                    if typ = typedefofIFSNode || typ = typedefofIFSRelationship
                    then typ
                    else pi.ParameterType
                else pi.ParameterType

            match expr with
            | NewObject (ci, paramsExpr) when isTyp ci ->
                let ctTypes = ci.GetParameters() |> Array.map getParamType
                Some (ctTypes, paramsExpr)
            | _ -> None

        let (|BuildJoin|_|) (operator : Operators) expr =
            match expr with
            | Call (_, mi, [ left; right ]) when mi.Name = operator.Name -> Some (operator, left, right)
            | _ -> None

        let rec inner (expr : Expr) =
            match expr with
            | Coerce (expr, _) | Let (_, _, expr) | TupleGet (expr, _) | Lambda (_, expr) -> inner expr
            | GetConstructors typedefofNode (ctTypes, paramsExpr) -> makeNode ctTypes paramsExpr
            | GetConstructors typedefofRel (ctTypes, paramsExpr) -> makeRel ctTypes paramsExpr
            | BuildJoin OpMMMM (operator, left, right)
            | BuildJoin OpLMMMM (operator, left, right)
            | BuildJoin OpMMMMG (operator, left, right)
            | BuildJoin OpMM (operator, left, right)
            | BuildJoin OpLMM (operator, left, right)
            | BuildJoin OpMMG (operator, left, right) ->
                inner left
                stepBuilder.AddStatement (string operator)
                inner right
            | Var v -> invalidOp (sprintf "You must call Node(..) or Rel(..) for Variable %s within the MATCH statement" v.Name)
            | _ -> invalidOp (sprintf "Unable to build MATCH statement from expression: %A" expr)

        inner expr

module private WhereSetClause =
    // TODO : full setting of node / rel
    open Helpers

    let make (stepBuilder : StepBuilder) (varDic : VarDic) (expr : Expr) =

        let buildState fExpr left symbol right =
            fExpr left
            stepBuilder.AddStatement " "
            stepBuilder.AddStatement symbol
            stepBuilder.AddStatement " "
            fExpr right

        let (|Operator|_|) (operator : Operators) fExpr expr =
            match expr with
            | Call (_, mi, [ left; right ]) when mi.Name = operator.Name ->
                Some (buildState fExpr left (string operator) right)
            | _ -> None

        let rec inner (expr : Expr) =
            match expr with
            | Let (_, _, expr) | Lambda (_, expr) -> inner expr
            | Operator OpEqual inner finalState
            | Operator OpLess inner finalState
            | Operator OpLessOrEqual inner finalState
            | Operator OpGreater inner finalState
            | Operator OpGreaterOrEqual inner finalState
            | Operator OpNotEqual inner finalState -> finalState
            | IfThenElse (left, right, Value(_, _)) -> buildState inner left "AND" right
            | IfThenElse (left, Value(_, _), right) -> buildState inner left "OR" right
            | IsIFSLabelTuple res -> RemoveClause.extractStatement stepBuilder varDic res
            | NewTuple exprs -> exprs |> List.iteri (StepBuilder.JoinTuple inner stepBuilder)
            | NewUnionCase (_, [ singleCase ]) -> inner singleCase
            | NewUnionCase (ui, []) when ui.Name = "None" -> stepBuilder.AddType expr
            | NewUnionCase (ui, _) when ui.Name = "Cons" || ui.Name = "Empty" -> stepBuilder.AddType expr
            | NewArray (_, _) -> stepBuilder.AddType expr
            | Value (_, _) -> stepBuilder.AddType expr
            | PropertyGet (Some (PropertyGet (Some e, pi, _)), _, _) ->
                inner e
                stepBuilder.AddStatement "."
                stepBuilder.AddStatement pi.Name
            | PropertyGet (Some e, pi, _) ->
                inner e
                stepBuilder.AddStatement "."
                stepBuilder.AddStatement pi.Name
            | PropertyGet (None, _, _) -> stepBuilder.AddType expr
            | Var v -> stepBuilder.AddStatement v.Name
            | _ -> invalidOp (sprintf "WHERE/SET statement - unmatched Expr: %A" expr)

        inner expr

module private ReturnClause =

    open Helpers

    let make<'T> (deserialize : Deserializer) (stepBuilder : StepBuilder) (expr : Expr) =

        let maker (expr : Expr) =
            match expr with
            | Value (o, typ) ->
                let key = StepBuilder.KeySymbol + stepBuilder.AddTypeRtnKey o
                key, typ
            | Var v ->
                stepBuilder.AddStatement v.Name
                v.Name, v.Type
            | PropertyGet (Some (Var v), pi, _) ->
                stepBuilder.AddStatement v.Name
                stepBuilder.AddStatement "."
                stepBuilder.AddStatement pi.Name
                v.Name + "." + pi.Name, pi.PropertyType
            | PropertyGet (None, pi, []) ->
                stepBuilder.AddStatement pi.Name
                pi.Name, pi.PropertyType
            | AS (v, fStatement) ->
                fStatement stepBuilder
                v.Name, v.Type.GenericTypeArguments.[0]
            | _ ->  invalidOp (sprintf "RETURN. Couldn't match expression: %A" expr)

        let rec inner (expr : Expr) =
            match expr with
            | Let (_, _, expr) | Lambda (_, expr) -> inner expr
            | Value _ | Var _ | PropertyGet _ | AS (_, _) -> maker expr |> Choice1Of2
            | NewTuple exprs ->
                exprs
                |> List.mapi (StepBuilder.JoinTuple maker stepBuilder)
                |> Choice2Of2
            | _ -> sprintf "RETURN. Unrecognized expression: %A" expr |> invalidOp

        let result = inner expr // Must run, otherwise never builds RETURN
        let continuation (di : Generic.IReadOnlyDictionary<string,obj>) =
            match result with
            | Choice1Of2 keyTyp -> deserialize di keyTyp
            | Choice2Of2 keyTyps ->
                keyTyps
                |> List.map (deserialize di)
                |> Expr.NewTuple
            |> Expr.Cast<'T>
            |> QuotationEvaluator.Evaluate

        Some continuation

[<AutoOpen>]
module CypherBuilder =

    // Initial help came from this great article by Thomas Petricek
    // http://tomasp.net/blog/2015/query-translation/
    // Other helpful articles
    // https://stackoverflow.com/questions/23122639/how-do-i-write-a-computation-expression-builder-that-accumulates-a-value-and-als
    // https://stackoverflow.com/questions/14110532/extended-computation-expressions-without-for-in-do
    // https://github.com/dotnet/fsharp/blob/master/src/fsharp/FSharp.Core/Query.fs

    [<NoComparison; NoEquality>]
    type ForEachQuery<'T> = private | FEQ

    [<NoComparison; NoEquality>]
    type Query<'T,'Result> = private | Q

    /// <summary>Supports the following commands:
    /// <para>CREATE, DELETE, FOREACH, MERGE, SET</para>
    /// </summary>
    type ForEach () =

        [<CustomOperation(nameof Clause.CREATE, MaintainsVariableSpace = true)>]
        member _.CREATE (source : ForEachQuery<'T>, [<ProjectionParameter>] statement : 'T -> Node<'N>) : ForEachQuery<'T> = FEQ

        [<CustomOperation(nameof Clause.DELETE, MaintainsVariableSpace = true)>]
        member _.DELETE (source : ForEachQuery<'T>, [<ProjectionParameter>] statement : 'T -> 'Delete) : ForEachQuery<'T> = FEQ

        [<CustomOperation(nameof Clause.DETACH_DELETE, MaintainsVariableSpace = true)>]
        member _.DETACH_DELETE (source : ForEachQuery<'T>, [<ProjectionParameter>] statement : 'T -> 'Delete) : ForEachQuery<'T> = FEQ

        [<CustomOperation(nameof Clause.MERGE, MaintainsVariableSpace = true)>]
        member _.MERGE (source : ForEachQuery<'T>, [<ProjectionParameter>] statement : 'T -> Node<'N>) : ForEachQuery<'T> = FEQ

        [<CustomOperation(nameof Clause.ON_CREATE_SET, MaintainsVariableSpace = true)>]
        member _.ON_CREATE_SET (source : ForEachQuery<'T>, [<ProjectionParameter>] statement : 'T -> 'Set) : ForEachQuery<'T> = FEQ

        [<CustomOperation(nameof Clause.ON_MATCH_SET, MaintainsVariableSpace = true)>]
        member _.ON_MATCH_SET (source : ForEachQuery<'T>, [<ProjectionParameter>] statement : 'T -> 'Set) : ForEachQuery<'T> = FEQ

        [<CustomOperation(nameof Clause.SET, MaintainsVariableSpace = true)>]
        member _.SET (source : ForEachQuery<'T>, [<ProjectionParameter>] statement : 'T -> 'Set) : ForEachQuery<'T> = FEQ

        member _.Yield (source : 'T) : ForEachQuery<'T> = FEQ

        member _.Zero () : ForEachQuery<'T> = FEQ

        member _.For (source : ForEachQuery<'T>, body : 'T -> ForEachQuery<'T>) : ForEachQuery<'T> = FEQ

        member _.For (source : AS<#IFSEntity<'T> list>, body : 'T -> ForEachQuery<'T>) : ForEachQuery<'T> = FEQ
        
        member _.For (source : #IFSEntity<'T> list, body : 'T -> ForEachQuery<'T>) : ForEachQuery<'T> = FEQ

        member _.Quote (source : Expr<ForEachQuery<'T>>) = FEQ

        member _.Run (source : Expr<ForEachQuery<'T>>) : unit = ()

    let FOREACH = ForEach()

    type CypherBuilder (serializer, deserializer) =
        let serializer : Serializer = Option.defaultValue Serialization.serialize serializer
        let deserializer : Deserializer = Option.defaultValue Deserialization.deserialize deserializer

        [<CustomOperation(nameof Clause.ASC, MaintainsVariableSpace = true)>]
        member _.ASC (source : Query<'T,'Result>) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.CREATE, MaintainsVariableSpace = true)>]
        member _.CREATE (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> Node<'N>) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.DELETE, MaintainsVariableSpace = true)>]
        member _.DELETE (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Delete) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.DESC, MaintainsVariableSpace = true)>]
        member _.DESC (source : Query<'T,'Result>) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.DETACH_DELETE, MaintainsVariableSpace = true)>]
        member _.DETACH_DELETE (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Delete) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.LIMIT, MaintainsVariableSpace = true)>]
        member _.LIMIT (source : Query<'T,'Result>, count : int64) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.MATCH, MaintainsVariableSpace = true)>]
        member _.MATCH (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> Node<'N>) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.MERGE, MaintainsVariableSpace = true)>]
        member _.MERGE (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> Node<'N>) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.ON_CREATE_SET, MaintainsVariableSpace = true)>]
        member _.ON_CREATE_SET (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Set) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.ON_MATCH_SET, MaintainsVariableSpace = true)>]
        member _.ON_MATCH_SET (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Set) : Query<'T,'Result> = Q

        // TODO: Look at how to handle the possiblitly of getting null into a result set or passing option types into the Node<'T>
        /// Note this can return null
        [<CustomOperation(nameof Clause.OPTIONAL_MATCH, MaintainsVariableSpace = true)>]
        member _.OPTIONAL_MATCH (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> Node<'N>) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.ORDER_BY, MaintainsVariableSpace = true)>]
        member _.ORDER_BY (source : Query<'T,'Result>, [<ProjectionParameter>] f : 'T -> 'Key) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.RETURN, MaintainsVariableSpace = true)>]
        member _.RETURN (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'FinalResult) : Query<'T,'FinalResult> = Q

        [<CustomOperation(nameof Clause.RETURN_DISTINCT, MaintainsVariableSpace = true)>]
        member _.RETURN_DISTINCT (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'FinalResult) : Query<'T,'FinalResult> = Q

        [<CustomOperation(nameof Clause.SET, MaintainsVariableSpace = true)>]
        member _.SET (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Set) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.REMOVE, MaintainsVariableSpace = true)>]
        member _.REMOVE (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'Remove) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.SKIP, MaintainsVariableSpace = true)>]
        member _.SKIP (source : Query<'T,'Result>, count : int64) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.UNION, MaintainsVariableSpace = true)>]
        member _.UNION (source : Query<'T,'Result>) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.UNION_ALL, MaintainsVariableSpace = true)>]
        member _.UNION_ALL (source : Query<'T,'Result>) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.UNWIND, MaintainsVariableSpace = true)>]
        member _.UNWIND (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'AS list) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.WHERE, MaintainsVariableSpace = true)>]
        member _.WHERE (source : Query<'T,'Result>, [<ProjectionParameter>] predicate : 'T -> bool) : Query<'T,'Result> = Q

        [<CustomOperation(nameof Clause.WITH, MaintainsVariableSpace = true)>]
        member _.WITH (source : Query<'T,'Result>, [<ProjectionParameter>] statement : 'T -> 'With) : Query<'T,'Result> = Q

        member _.Yield (source : 'T) : Query<'T,unit> = Q

        member _.Zero () : Query<'T,unit> = Q

        member _.For (source : Query<'T,'Result>, body : 'T -> Query<'T2, unit>) : Query<'T2, unit> = Q

        member _.For (source : Node<'T>, body : 'T -> Query<'T2, unit>) : Query<'T2, unit> = Q

        member _.For (source : Rel<'T>, body : 'T -> Query<'T2, unit>) : Query<'T2, unit> = Q

        member _.Quote (source : Expr<Query<'T,'Result>>) = Q

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
                    | Let (v, e1, e2) ->
                        if not(varDic.ContainsKey v.Name)
                        then
                            varDic.Add(v.Name, if varExp.IsSome then varExp.Value else e1)
                            varExp <- None
                        inner e2
                    | ShapeCombination (_, exprs) -> List.iter inner exprs
                    | ShapeLambda (_, expr) -> inner expr
                    | ShapeVar _ -> ()
                inner expr.Raw
                varDic

            let (|MatchCreateMerge|_|) (callExpr : Expr) (stepBuilder : StepBuilder) (expr : Expr) =
                match expr with
                | SpecificCall callExpr (_, _, [ stepAbove; thisStep ]) ->
                    MatchClause.make stepBuilder varDic thisStep
                    Some stepAbove
                | _ -> None

            let (|WhereSet|_|) (callExpr : Expr) (stepBuilder : StepBuilder) (expr : Expr) =
                match expr with
                | SpecificCall callExpr (_, _, [ stepAbove; thisStep ]) ->
                    WhereSetClause.make stepBuilder varDic thisStep
                    Some stepAbove
                | _ -> None

            let mutable returnStatement : ReturnContination<'Result> option = None

            let (|Return|_|) (callExpr : Expr) (stepBuilder : StepBuilder) (expr : Expr) =
                match expr with
                | SpecificCall callExpr (_, _, [ stepAbove; thisStep ]) ->
                    returnStatement <- ReturnClause.make<'Result> deserializer stepBuilder thisStep
                    Some stepAbove
                | _ -> None

            let (|Basic|_|) (callExpr : Expr) (stepBuilder : StepBuilder) (expr : Expr) =
                match expr with
                | SpecificCall callExpr (_, _, [ stepAbove; thisStep ]) ->
                    BasicClause.make stepBuilder thisStep
                    Some stepAbove
                | _ -> None

            let (|Unwind|_|) (stepBuilder : StepBuilder) (expr : Expr) =
                match expr with
                | SpecificCall <@@ this.UNWIND @@> (_, _, [ stepAbove; thisStep ]) ->
                    UnwindClause.make stepBuilder thisStep
                    Some stepAbove
                | _ -> None

            let (|NoStatement|_|) (callExpr : Expr) (expr : Expr) =
                match expr with
                | SpecificCall callExpr (_, _, [ stepAbove ]) -> Some stepAbove
                | _ -> None

            let (|Remove|_|) (stepBuilder : StepBuilder) (expr : Expr) =
                match expr with
                | SpecificCall <@@ this.REMOVE @@> (_, _, [ stepAbove; thisStep ]) -> 
                    RemoveClause.make stepBuilder varDic thisStep
                    Some stepAbove
                | _ -> None
                | _ -> None

            let (|ForEachStatement|_|) buildForEach (stepBuilder : StepBuilder) (expr : Expr) =
                let rec inner expr =
                    match expr with
                    | Let (_, _, expr) | Lambda (_, expr) -> inner expr
                    | Sequential (Application (Lambda (var, expr) , _) , _) when var.Type = typeof<ForEach> ->
                        buildForEach stepBuilder expr
                        stepBuilder.FinaliseClause Clause.FOREACH
                        Some ()
                    | _ -> None

                inner expr

            // Accumulate outside of the loop & mutate due to build order
            let mutable firstStatement = ""
            let mutable isFirstStatement = true

            let rec buildForEach (stepBuilder : StepBuilder) (expr : Expr) =
                firstStatement <- firstStatement + ")"
                let rec inner (expr : Expr) =
                    let moveToNext clause next =
                        if isFirstStatement then
                            stepBuilder.AddStatement firstStatement
                            isFirstStatement <- false

                        stepBuilder.FinaliseClause clause
                        inner next

                    match expr with
                    | SpecificCall <@@ FOREACH.Run @@> (_, _, [ expr ]) // Always called first
                    | QuoteTyped expr | Let (_, _, expr) | Lambda (_, expr) | Coerce (expr, _)
                    | Sequential (_, expr) -> inner expr
                    | SpecificCall <@@ FOREACH.Yield @@> (_, _, [ expr ]) -> inner expr
                    | SpecificCall <@@ FOREACH.Zero @@> _ -> ()
                    | Var v -> stepBuilder.AddStatement v.Name
                    | Call (_, mi, [ stepAbove; ForEachStatement buildForEach stepBuilder ]) when mi.Name = "For" -> inner stepAbove
                    | Call (_, mi, [ stepAbove; thisStep ]) when mi.Name = "For" ->
                        stepBuilder.AddStatement "("
                        inner thisStep
                        stepBuilder.AddStatement " IN "
                        inner stepAbove
                        stepBuilder.AddStatement " |"
                    | MatchCreateMerge <@@ FOREACH.CREATE @@> stepBuilder stepAbove -> moveToNext Clause.CREATE stepAbove
                    | Basic <@@ FOREACH.DELETE @@> stepBuilder stepAbove -> moveToNext Clause.DELETE stepAbove
                    | Basic <@@ FOREACH.DETACH_DELETE @@> stepBuilder stepAbove -> moveToNext Clause.DETACH_DELETE stepAbove
                    | MatchCreateMerge <@@ FOREACH.MERGE @@> stepBuilder stepAbove -> moveToNext Clause.MERGE stepAbove
                    | WhereSet <@@ FOREACH.ON_CREATE_SET @@> stepBuilder stepAbove -> moveToNext Clause.ON_CREATE_SET stepAbove
                    | WhereSet <@@ FOREACH.ON_MATCH_SET @@> stepBuilder stepAbove -> moveToNext Clause.ON_MATCH_SET stepAbove
                    | WhereSet <@@ FOREACH.SET @@> stepBuilder stepAbove -> moveToNext Clause.SET stepAbove
                    | _ -> invalidOp (sprintf "FOREACH. Unmatched Expr: %A" expr)

                inner expr

            let stepBuilder = StepBuilder serializer

            let rec buildQry (expr : Expr) =

                let moveToNext clause next =
                    stepBuilder.FinaliseClause clause
                    buildQry next

                match expr with
                | SpecificCall <@@ this.Yield @@> _ -> ()
                | Call (_, mi, [ stepAbove; ForEachStatement buildForEach stepBuilder ]) when mi.Name = "For" -> buildQry stepAbove
                | Call (_, mi, _) when mi.Name = "For" -> ()
                | Let (_, _, expr) | Lambda (_, expr) -> buildQry expr
                | NoStatement <@@ this.ASC @@> stepAbove -> moveToNext Clause.ASC stepAbove
                | MatchCreateMerge <@@ this.CREATE @@> stepBuilder stepAbove -> moveToNext Clause.CREATE stepAbove
                | Basic <@@ this.DELETE @@> stepBuilder stepAbove -> moveToNext Clause.DELETE stepAbove
                | NoStatement <@@ this.DESC @@> stepAbove -> moveToNext Clause.DESC stepAbove
                | Basic <@@ this.DETACH_DELETE @@> stepBuilder stepAbove -> moveToNext Clause.DETACH_DELETE stepAbove
                | Basic <@@ this.LIMIT @@> stepBuilder stepAbove -> moveToNext Clause.LIMIT stepAbove
                | MatchCreateMerge <@@ this.MATCH @@> stepBuilder stepAbove -> moveToNext Clause.MATCH stepAbove
                | MatchCreateMerge <@@ this.MERGE @@> stepBuilder stepAbove -> moveToNext Clause.MERGE stepAbove
                | WhereSet <@@ this.ON_CREATE_SET @@> stepBuilder stepAbove -> moveToNext Clause.ON_CREATE_SET stepAbove
                | WhereSet <@@ this.ON_MATCH_SET @@> stepBuilder stepAbove -> moveToNext Clause.ON_MATCH_SET stepAbove
                | MatchCreateMerge <@@ this.OPTIONAL_MATCH @@> stepBuilder stepAbove -> moveToNext Clause.OPTIONAL_MATCH stepAbove
                | Basic <@@ this.ORDER_BY @@> stepBuilder stepAbove -> moveToNext Clause.ORDER_BY stepAbove
                | Remove stepBuilder stepAbove -> moveToNext Clause.REMOVE stepAbove
                | Return <@@ this.RETURN @@> stepBuilder stepAbove -> moveToNext Clause.RETURN stepAbove
                | Return <@@ this.RETURN_DISTINCT @@> stepBuilder stepAbove -> moveToNext Clause.RETURN_DISTINCT stepAbove
                | WhereSet <@@ this.SET @@> stepBuilder stepAbove -> moveToNext Clause.SET stepAbove
                | Basic <@@ this.SKIP @@> stepBuilder stepAbove -> moveToNext Clause.SKIP stepAbove
                | NoStatement <@@ this.UNION @@> stepAbove -> moveToNext Clause.UNION stepAbove
                | NoStatement <@@ this.UNION_ALL @@> stepAbove -> moveToNext Clause.UNION_ALL stepAbove
                | Unwind stepBuilder stepAbove -> moveToNext Clause.UNWIND stepAbove
                | WhereSet <@@ this.WHERE @@> stepBuilder stepAbove -> moveToNext Clause.WHERE stepAbove
                | Basic <@@ this.WITH @@>stepBuilder stepAbove -> moveToNext Clause.WITH stepAbove
                | _ -> sprintf "Un matched method when building Query: %A" expr |> invalidOp

            buildQry expr.Raw

            stepBuilder.Build returnStatement

    let cypher = CypherBuilder(None, None)