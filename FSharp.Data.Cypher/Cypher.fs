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

type VarDic = Generic.Dictionary<string,Expr>

module ClauseNames =
    
    let [<Literal>] MATCH = "MATCH"
    let [<Literal>] OPTIONAL_MATCH = "OPTIONAL_MATCH"
    let [<Literal>] CREATE = "CREATE"
    let [<Literal>] MERGE = "MERGE"
    let [<Literal>] WHERE = "WHERE"
    let [<Literal>] SET = "SET"
    let [<Literal>] RETURN = "RETURN"
    let [<Literal>] RETURN_DISTINCT = "RETURN_DISTINCT"
    let [<Literal>] ORDER_BY = "ORDER_BY"
    let [<Literal>] SKIP = "SKIP"
    let [<Literal>] LIMIT = "LIMIT"
    // TODO - here for write/read check completeness
    let [<Literal>] DELETE = "DELETE"
    let [<Literal>] DETACH_DELETE = "DETACH_DELETE"
    let [<Literal>] REMOVE = "REMOVE"
    let [<Literal>] FOREACH = "FOREACH"

type Clause = 
    | MATCH
    | OPTIONAL_MATCH
    | CREATE
    | MERGE
    | WHERE
    | SET
    | RETURN
    | RETURN_DISTINCT
    | ORDER_BY
    | SKIP
    | LIMIT
    | DELETE
    | DETACH_DELETE
    | REMOVE
    | FOREACH
    override this.ToString() =
        match this with
        | MATCH -> ClauseNames.MATCH
        | OPTIONAL_MATCH -> ClauseNames.OPTIONAL_MATCH
        | CREATE -> ClauseNames.CREATE
        | MERGE -> ClauseNames.MERGE
        | WHERE -> ClauseNames.WHERE
        | SET -> ClauseNames.SET
        | RETURN -> ClauseNames.RETURN
        | RETURN_DISTINCT -> ClauseNames.RETURN_DISTINCT
        | ORDER_BY -> ClauseNames.ORDER_BY
        | SKIP -> ClauseNames.SKIP
        | LIMIT -> ClauseNames.LIMIT
        | DELETE -> ClauseNames.DELETE
        | DETACH_DELETE -> ClauseNames.DETACH_DELETE
        | REMOVE -> ClauseNames.REMOVE
        | FOREACH -> ClauseNames.FOREACH
        |> fun s -> s.Replace("_", " ")
    member this.IsWrite =   
        match this with
        | CREATE | MERGE | SET | DELETE | DETACH_DELETE | REMOVE | FOREACH -> true
        | _ -> false
    member this.IsRead = not(this.IsWrite)
        
module Clause =
    
    let isRead (clause : Clause) = clause.IsRead

    let isWrite (clause : Clause) = clause.IsWrite

type QueryResult<'T>(results : 'T [], summary : IResultSummary) =
    member __.Results = results
    member __.Summary = summary

module QueryResult =

    let results (cr : QueryResult<'T>) = cr.Results

    let summary (cr : QueryResult<'T>) = cr.Summary

type TransactionResult<'T>(results : 'T [], summary : IResultSummary, session : ISession, transaction : ITransaction) =
    member __.Results = results
    member __.Summary = summary
    member __.Session = session
    member __.Transaction = transaction
    member this.AsyncCommit() = 
        async {
            do! this.Transaction.CommitAsync() |> Async.AwaitTask
            this.Session.Dispose()
            return QueryResult(this.Results, this.Summary)
        }
    member this.Commit() = this.AsyncCommit() |> Async.RunSynchronously
    member this.AsyncRollback() =
        async {
            do! this.Transaction.RollbackAsync() |> Async.AwaitTask
            this.Session.Dispose()
            return ()
        }
    member this.Rollback() = this.AsyncRollback() |> Async.RunSynchronously

module TransactionResult =

    let results (cr : TransactionResult<'T>) = cr.Results
    
    let summary (cr : TransactionResult<'T>) = cr.Summary
    
    let transaction (cr : TransactionResult<'T>) = cr.Transaction

    let commit (cr : TransactionResult<'T>) = cr.Commit()
    
    let rollback (cr : TransactionResult<'T>) = cr.Rollback()

    let asyncCommit (cr : TransactionResult<'T>) = cr.AsyncCommit()
    
    let asyncRollback (cr : TransactionResult<'T>) = cr.AsyncRollback()

type CypherStep =
    | NonParameterized of Clause : Clause * Statement : string
    | Parameterized of Clause : Clause * Statement : Choice<string,string -> (string * obj option)> list
    member this.Clause =
        match this with
        | NonParameterized (c, _) -> c
        | Parameterized (c, _) -> c
    static member FixStringParameter (s : string) = sprintf "\"%s\"" s
    static member FixStringParameter (typ : Type, o : obj) = 
        if typ = typeof<string> then o :?> string |> CypherStep.FixStringParameter 
        else string o
     static member Paramkey = "$"
   
module CypherStep =
    
    let buildQuery (steps : CypherStep list) =
        let mutable prmList = List.Empty
        let makeParms (c : Clause ) (stepCount : int) (prms : Choice<string,string -> (string * obj option)> list) =
            let mutable pCount = 1
            prms
            |> List.map (fun x ->
                match x with
                | Choice1Of2 s -> s
                | Choice2Of2 f -> 
                    let s = "step" + string (stepCount + 1) + "param" + string pCount
                    pCount <- pCount + 1
                    prmList <- f s :: prmList
                    CypherStep.Paramkey + s)
            |> String.concat ""
            |> sprintf "%s %s" (string c)

        steps
        |> List.mapi (fun i x -> 
            match x with
            | NonParameterized (c, s) -> sprintf "%s %s" (string c) s
            | Parameterized (c, prms) -> makeParms c i prms)
        |> fun s -> s, prmList

type Cypher<'T>(querySteps : CypherStep list, continuation : Generic.IReadOnlyDictionary<string, obj> -> 'T) =
    let Paramkey = "$"
    let (query, prms) = CypherStep.buildQuery querySteps
    member __.QuerySteps = querySteps
    member __.Continuation = continuation
    member __.Parameters = prms
    member __.Query = String.concat " " query
    member __.QueryMultiline = String.concat Environment.NewLine query 
    member __.IsWrite = querySteps |> List.exists (fun x -> x.Clause.IsWrite)
    member this.QueryNonParameterized = // TODO : should build this at the same time as the paramterized query
        (this.Query, this.Parameters)
        ||> List.fold (fun state (k, o) -> 
            let v =
                match o with
                | Some o ->
                    match o with
                    | :? string as s -> CypherStep.FixStringParameter s
                    | :? Generic.List<obj> as xs ->
                        xs
                        |> Seq.map string
                        |> String.concat ", "
                        |> sprintf "[ %s ]"
                    | :? Generic.Dictionary<string, obj> as d ->
                        d 
                        |> Seq.map (fun kv -> kv.Key + " : " + CypherStep.FixStringParameter(kv.Value.GetType(), kv.Value))
                        |> String.concat ", "
                        |> sprintf "{ %s }"
                    | _ -> string o
                | None -> "null"

            state.Replace(Paramkey + k, v))

module Cypher =

    // Neo4j Driver is not happy unless this is Dictionary - doesn't like some F# collections even though implement IDictionary
    // it will give Neo4j.Driver.V1.ServiceUnavailableException: Unexpected end of stream, read returned 0
    // private as it introduces null
    let private makeParameters (cypher : Cypher<'T>) =
        cypher.Parameters
        |> List.map (fun (k, v) -> k, if v.IsNone then null else v.Value)
        |> dict
        |> Generic.Dictionary
    
    // https://neo4j.com/docs/driver-manual/1.7/sessions-transactions/#driver-transactions-access-mode
    // Should I use array / parallel here? Lots of reflecion so may be worth while? 
    let private runTransaction (driver : IDriver) (map : 'T -> 'U) (cypher : Cypher<'T>) =
        use session = driver.Session()
        try
            let run (t : ITransaction) = t.Run(cypher.Query, makeParameters cypher)
            let sr = if cypher.IsWrite then session.WriteTransaction run else session.ReadTransaction run
            let results = 
                sr 
                |> Seq.toArray 
                |> Array.Parallel.map (fun record -> cypher.Continuation record.Values |> map) 
            QueryResult(results, sr.Summary)
        finally
            session.CloseAsync()
            |> Async.AwaitTask
            |> Async.RunSynchronously 
            |> ignore 

    let runMap (driver : IDriver) map cypher = runTransaction driver map cypher

    let run (driver : IDriver) cypher = runMap driver id cypher

    let asyncRun driver cypher = async { return run driver cypher }

    let asyncRunMap driver map cypher = async { return runMap driver map cypher }

    let spoof (di : Generic.IReadOnlyDictionary<string, obj>) (cypher : Cypher<'T>) = cypher.Continuation di

    let queryNonParameterized (cypher : Cypher<'T>) = cypher.QueryNonParameterized

    module Explicit =
    
        let private runTransaction (session : ISession) (map : 'T -> 'U) (cypher : Cypher<'T>) =
            let transaction = session.BeginTransaction()
            let sr = transaction.Run(cypher.Query, makeParameters cypher)
            let results = 
                sr 
                |> Seq.toArray 
                |> Array.Parallel.map (fun record -> cypher.Continuation record.Values |> map) 
            TransactionResult(results, sr.Summary, session, transaction)
        
        let runMap (driver : IDriver) map (cypher : Cypher<'T>) =
            use session = if cypher.IsWrite then driver.Session AccessMode.Write else driver.Session AccessMode.Read
            try runTransaction session map cypher
            with e ->
                session.CloseAsync()
                |> Async.AwaitTask
                |> Async.RunSynchronously 
                |> ignore  
                raise e

        let run (driver : IDriver) cypher = runMap driver id cypher

        let asyncRun driver cypher = async { return run driver cypher }

        let asyncRunMap driver map cypher = async { return runMap driver map cypher }

module private Loggers =

    let logger (stepCount : int) (specifCallName : string) (stepAbove : Expr) (thisStep : Expr) =
        printfn "Step: %i. Step Name: %s" stepCount specifCallName
        printfn "This step:"
        printfn "%A"  thisStep
        printfn "Steps above:" 
        printfn "%A" stepAbove

module private MatchClause =
    
    let [<Literal>] private IFSNode = "IFSNode"
    let [<Literal>] private IFSRelationship = "IFSRelationship"

    module Rel =
        
        let getCtrParamsTypes (ci : ConstructorInfo) = ci.GetParameters() |> Array.map (fun x -> x.ParameterType)

        let makeIFSRel (expr : Expr) =
            match expr with
            | Coerce (PropertyGet (_, rel, _), _) -> rel.Name
            | Coerce (Var rel, _) -> rel.Name
            | _ -> 
                sprintf "Could not build IFSRelationship from expression %A" expr
                |> invalidOp

        let makeRelLabel (varDic : VarDic) (expr : Expr) =
            let rec inner (expr : Expr) =
                match expr with
                | Var rel -> QuotationEvaluator.EvaluateUntyped varDic.[rel.Name] :?> RelLabel |> string
                | PropertyGet (None, _, []) -> QuotationEvaluator.EvaluateUntyped expr :?> RelLabel |> string
                | PropertyGet (Some (Var v), pi, []) ->
                    let t = FSharpValue.MakeRecord(v.Type, Array.zeroCreate (FSharpType.GetRecordFields v.Type).Length)
                    pi.GetValue t :?> RelLabel |> string
                | Value (obj, _) -> obj :?> RelLabel |> string
                | NewObject (_, [ _ ] ) -> QuotationEvaluator.EvaluateUntyped expr :?> RelLabel |> string
                | SpecificCall <@ (/) @> (_, _, xs) -> List.map inner xs |> String.concat "|"
                | _ -> sprintf "Could not build RelLabel from expression %A" expr |> invalidOp
                
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


    let make (varDic : VarDic) expr =

        let makeNodeLabel nodeLabel =
            match nodeLabel with
            | PropertyGet (_, _, _) -> QuotationEvaluator.EvaluateUntyped nodeLabel :?> NodeLabel |> string 
            | NewObject (ci, [ Value (o, t) ]) when ci.DeclaringType = typeof<NodeLabel> -> NodeLabel (string o) |> string
            | NewUnionCase (ui, _) when ui.Name = "Cons" ->
                QuotationEvaluator.EvaluateUntyped nodeLabel
                :?> NodeLabel list
                |> List.map string
                |> String.concat ""
            | _ -> invalidOp "Could not build node label(s)"
            |> string

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

        let getJoinSymbol (onNodes : string) onRel left right =
            match left, right with
            | Coerce (NewObject (ci1, _), _), Coerce (NewObject (ci2, _), _) when ci1.DeclaringType = typeof<Node> && ci2.DeclaringType = typeof<Node> -> onNodes
            | _ -> onRel


        // NewObject, the number of items in the list matches the no. parameters passed to the constructor
        // Coarce can let you get the instance of the type when evaluated
        // Var is for items inside the expression
        // PropertyGet is for items outside the expression
        let rec inner (expr : Expr) =
            match expr with
            | Rel.Relationship varDic str -> str

            | NewObject (ci, []) when ci.DeclaringType = typeof<Node> -> "()"
            // inside comp, for .. in
            | NewObject (ci, [ Coerce (Var node, _) ]) when ci.DeclaringType = typeof<Node> -> sprintf "(%s)" node.Name
            
            // Outside comp
            | NewObject (ci, [ Coerce (PropertyGet (_, node, _), _) ]) when ci.DeclaringType = typeof<Node> -> sprintf "(%s)" node.Name
            
            | NewObject (ci, [ Coerce (eNode, _); e2 ]) when ci.DeclaringType = typeof<Node> -> // Need eNode to evaluate so can do a comparison
                let hasProperties =
                    let ctr = ci.GetParameters() |> Array.map (fun x -> x.ParameterType)
                    match ctr with
                    | xs when xs = [| typeof<IFSNode>; typeof<NodeLabel> |] || xs = [| typeof<IFSNode>; typeof<NodeLabel list> |] -> false
                    | xs when xs = [| typeof<IFSNode>; typeof<IFSNode> |] -> true
                    | _ -> invalidOp "invalid constructor found"
                match eNode with 
                | PropertyGet (_, node, _) -> 
                    if hasProperties 
                    then sprintf "(%s %s)" node.Name (makeNodeWithProperties eNode e2)
                    else sprintf "(%s%s)" node.Name (makeNodeLabel e2)
                | Var node -> 
                    if hasProperties 
                    then sprintf "(%s %s)" node.Name (makeNodeWithProperties eNode varDic.[node.Name])
                    else sprintf "(%s%s)" node.Name (makeNodeLabel e2)
                | _ -> 
                    printfn "%A" expr
                    printfn "%A" eNode
                    invalidOp "invalid node builder"

            | NewObject (ci, [ Coerce (eNode1, t); eLabels; eNode2]) when ci.DeclaringType = typeof<Node> -> 
                match eNode1 with 
                | PropertyGet (_, node, _) ->
                    sprintf "(%s%s %s)" node.Name (makeNodeLabel eLabels) (makeNodeWithProperties eNode1 eNode2)
                | _ -> invalidOp "invalid constructor found"
                
            | SpecificCall <@ (--) @> (_, _, [ leftSide; rightSide ]) -> inner leftSide + (getJoinSymbol "--" "-" leftSide rightSide) + inner rightSide
            | SpecificCall <@ (-->) @> (_, _, [ leftSide; rightSide ]) -> inner leftSide + (getJoinSymbol "-->" "->" leftSide rightSide) + inner rightSide
            | SpecificCall <@ (<--) @> (_, _, [ leftSide; rightSide ]) -> inner leftSide + (getJoinSymbol "<--" "<-" leftSide rightSide) + inner rightSide
            | Coerce (exp, typ) when typ = typeof<IFSEntity> -> inner exp
            | Let (v, e1, e2) -> 
                if not(varDic.ContainsKey v.Name) then varDic.Add(v.Name, e1) // Add the var expresssion for later evaluation
                inner e2
            | TupleGet (exp, _) -> inner exp
            | Lambda (_, exp) -> inner exp
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
        | Var v -> v.Name // string needs to be escaped
        | PropertyGet (Some (Var v), pi, _) -> makePropertyKey v pi
        // When variable is outside the builder
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
    open Loggers
    
    type Query<'T> = NA

    type private ReturnContination<'T> = 
        | ReturnContination of (Generic.IReadOnlyDictionary<string, obj> -> 'T)
        | Empty
        member this.Update v =
            match this with
            | ReturnContination _ -> invalidOp "Only 1 x RETURN clause is allowed in a Cypher query."
            | Empty -> ReturnContination v
        member this.Value =
            match this with
            | ReturnContination v -> v
            | Empty -> invalidOp "A Cypher query must contain a RETURN clause" // Not strictly true... DELETE doesn't

    type CypherBuilder() =
        
        // Need to define a yield method. It is used rather than zero in case there are variables in scope
        member __.Yield(source : 'T) : Query<'T> = NA

        // f needs a different type 'R to allow multiple for in do loops
        member __.For(source : Query<'T>, f : 'T -> Query<'R>) : Query<'R> = NA 
        
        // Cypher Queries
        // All match statements must end in a Node, one bug here if only use a single operator its still returns IFSNode
        [<CustomOperation(ClauseNames.MATCH, MaintainsVariableSpace = true)>]
        member __.MATCH(source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSNode) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.OPTIONAL_MATCH, MaintainsVariableSpace = true)>]
        member __.OPTIONAL_MATCH(source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSNode) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.CREATE, MaintainsVariableSpace = true)>]
        member __.CREATE(source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSNode) : Query<'T> = NA
        
        [<CustomOperation(ClauseNames.MERGE, MaintainsVariableSpace = true)>]
        member __.MERGE(source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSNode) : Query<'T> = NA
        
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

            // Not shoud probably pass these down in the state since they are all mutable
            let varDic = Generic.Dictionary<string,Expr>()
            let mutable returnStatement : ReturnContination<'T> = Empty
            let mutable varPropGet = None

            let buildQry (e : Expr<Query<'T>>) =
                let rec queryBuilder (state : CypherStep list) (e : Expr) =
                    match e with
                    | SpecificCall <@ cypher.Yield @> _ -> state
                    
                    | SpecificCall <@ cypher.For @> (_, _, [ pGet; yieldEnd ]) ->
                        varPropGet <- Some pGet
                        queryBuilder state yieldEnd
                    
                    | Let (v, e1, e2) -> 
                        varDic.Add(v.Name, if varPropGet.IsSome then varPropGet.Value else e1) // Add the expresssion for later evaluation
                        varPropGet <- None
                        queryBuilder state e2
                    
                    | Lambda (_, exp) -> queryBuilder state exp
                    
                    | SpecificCall <@ cypher.MATCH @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.MATCH stepAbove thisStep  
                        let statement = MatchClause.make varDic thisStep
                        let newState = NonParameterized(MATCH, statement) :: state
                        queryBuilder newState stepAbove

                    // TODO: This can RETURN some nulls.. need to look further into that
                    | SpecificCall <@ cypher.OPTIONAL_MATCH @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.OPTIONAL_MATCH stepAbove thisStep  
                        let statement = MatchClause.make varDic thisStep
                        let newState = NonParameterized(OPTIONAL_MATCH, statement) :: state
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.CREATE @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.CREATE stepAbove thisStep  
                        let statement = MatchClause.make varDic thisStep
                        let newState = NonParameterized(CREATE, statement) :: state
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.MERGE @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.MERGE stepAbove thisStep  
                        let statement = MatchClause.make varDic thisStep
                        let newState = NonParameterized(MERGE, statement) :: state
                        queryBuilder newState stepAbove
                    
                    | SpecificCall <@ cypher.WHERE @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.WHERE stepAbove thisStep  
                        let statement = WhereAndSetStatement.make thisStep
                        let newState = Parameterized(WHERE, statement) :: state
                        queryBuilder newState stepAbove
                    
                    | SpecificCall <@ cypher.SET @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.SET stepAbove thisStep  
                        let statement = WhereAndSetStatement.make thisStep
                        let newState = Parameterized(SET, statement) :: state
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.RETURN @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.RETURN stepAbove thisStep 
                        let (statement, continuation) = ReturnClause.make<'T> thisStep
                        let newState = NonParameterized(RETURN, statement) :: state
                        returnStatement <- returnStatement.Update continuation
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.RETURN_DISTINCT @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.RETURN_DISTINCT stepAbove thisStep 
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

                    | _ -> 
                        e
                        |> sprintf "Un matched method when building Query: %A"
                        |> invalidOp 

                queryBuilder [] e, returnStatement.Value
            
            Cypher(buildQry e)

    let cypher = CypherBuilder()