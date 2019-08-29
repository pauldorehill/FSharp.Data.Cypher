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
    | Parameterized of Clause : Clause * Statement : Choice<string,string -> string> list * Parameters : (string * obj option) list
    member this.Clause =
        match this with
        | NonParameterized (c, _) -> c
        | Parameterized (c, _, _) -> c
    //member this.Statement = statement
    //member this.Parameters = parameters
    //member this.CompleteStep = string clause + " " + statement
    static member FixStringParameter (s : string) = sprintf "\"%s\"" s
    static member FixStringParameter (typ : Type, o : obj) = 
        if typ = typeof<string> then o :?> string |> CypherStep.FixStringParameter 
        else string o

type Cypher<'T>(querySteps : CypherStep list, continuation : Generic.IReadOnlyDictionary<string, obj> -> 'T) =
    let MakeQuery sep =
        querySteps
        |> List.map (fun x -> x.CompleteStep)
        |> String.concat sep

    member __.QuerySteps = querySteps
    member __.Continuation = continuation
    member __.Parameters =
        querySteps
        |> List.choose (fun x -> x.Parameters)
        |> List.concat

    member __.Query = MakeQuery " "
    member __.QueryMultiline = MakeQuery Environment.NewLine
    member __.IsWrite = querySteps |> List.exists (fun x -> x.Clause.IsWrite)
    member this.QueryNonParameterized = // TODO : should capture the raw statement so no need to do this. Also match on other types e.g Generic.List
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

            state.Replace("$" + k, v))

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
    let private runTransaction (cypher : Cypher<'T>) (driver : IDriver) (map : 'T -> 'U) =
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

    let run driver cypher = runTransaction cypher driver id

    let runMap driver map cypher = runTransaction cypher driver map

    let asyncRun driver cypher = async { return runTransaction cypher driver id }

    let asyncRunMap driver map cypher = async { return runTransaction cypher driver map }

    let spoof (di : Generic.IReadOnlyDictionary<string, obj>) (cypher : Cypher<'T>) = cypher.Continuation di

    let queryNonParameterized (cypher : Cypher<'T>) = cypher.QueryNonParameterized

    module Explicit =
    
        let private runTransaction (cypher : Cypher<'T>) (driver : IDriver) (map : 'T -> 'U) =
            use session = if cypher.IsWrite then driver.Session AccessMode.Write else driver.Session AccessMode.Read
            try
                let transaction = session.BeginTransaction()
                let sr = transaction.Run(cypher.Query, makeParameters cypher)
                let results = 
                    sr 
                    |> Seq.toArray 
                    |> Array.Parallel.map (fun record -> cypher.Continuation record.Values |> map) 
                TransactionResult(results, sr.Summary, session, transaction)
            with e ->
                session.CloseAsync()
                |> Async.AwaitTask
                |> Async.RunSynchronously 
                |> ignore  
                raise e
        
        let run driver cypher = runTransaction cypher driver id

        let runMap driver map cypher = runTransaction cypher driver map

        let asyncRun driver cypher = async { return runTransaction cypher driver id }

        let asyncRunMap driver map cypher = async { return runTransaction cypher driver map }

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

    let makeLabels name (o : obj)  =
        let typ = o.GetType()
        if typ = typeof<IFSNode> || Deserialization.hasInterface typ IFSNode
        then 
            (o :?> IFSNode).Labels
            |> Option.map (fun xs ->
                xs
                |> List.map Label.make
                |> String.concat "")
            |> Option.defaultValue ""
            |> sprintf "(%s%s)" name

        elif typ = typeof<IFSRelationship> || Deserialization.hasInterface typ IFSRelationship
        then
            (o :?> IFSRelationship).Label
            |> Label.make
            |> sprintf "[%s%s]" name
        else 
            typ.Name
            |> sprintf "Tried to make labels, but not a %s or %s: %s" IFSNode IFSRelationship
            |> invalidOp 

    let buildAscii expr =
        let rec inner expr =
            //printfn "%A" expr
            match expr with
            | SpecificCall <@ (--) @> (_, _, [ leftSide; rightSide ]) -> inner leftSide + "--" + inner rightSide
            | SpecificCall <@ (-->) @> (_, _, [ leftSide; rightSide ]) -> inner leftSide + "-->" + inner rightSide
            | SpecificCall <@ (<--) @> (_, _, [ leftSide; rightSide ]) -> inner leftSide + "<--" + inner rightSide
            | SpecificCall <@ (-|) @> (_, _, [ leftSide; rightSide ]) -> inner leftSide + "-" + inner rightSide
            | SpecificCall <@ (|->) @> (_, _, [ leftSide; rightSide ]) -> inner leftSide + "->" + inner rightSide
            | SpecificCall <@ (|-) @> (_, _, [ leftSide; rightSide ]) -> inner leftSide + "-" + inner rightSide
            | SpecificCall <@ (<-|) @> (_, _, [ leftSide; rightSide ]) -> inner leftSide + "<-" + inner rightSide
            // Coarce can let you get the instance of the type
            // Wrapped like this to prevent case when its AsciiStep triggering makeLabels
            | Coerce (PropertyGet (_, pi, _), typ) -> 
                QuotationEvaluator.EvaluateUntyped expr
                |> makeLabels pi.Name
            | Coerce (ex, _) -> inner ex
            | Let (_, _, e2) -> inner e2
            | TupleGet (exp, _) -> inner exp
            | Var v -> 
                Deserialization.createNullRecordOrClass v.Type
                |> makeLabels v.Name //<@ o @> v.Type 
            | PropertyGet (_, pi, _) -> 
                QuotationEvaluator.EvaluateUntyped expr
                |> makeLabels pi.Name
            | _ -> 
                expr
                |> sprintf "MATCH. Unable to build ascii statement: %A"
                |> invalidOp
    
        inner expr

    let make (expr : Expr) =
        match expr with
        | Lambda (v, exp) -> buildAscii exp
        | _ -> 
            expr
            |> sprintf "MATCH. Step was not a Lambda as expected : %A"
            |> invalidOp
 
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

    let private rnd = Random()
    let private chars = "ABCDEFGHIJKLMNOPQRSTUVXYZabcdefghijklmnopqrstuvxyz".ToCharArray()

    // TODO : Make this a bit smarter. Should generate a key based of the inputs, so its testable // repeatable?
    // This could also have implications around query caching?
    // Parameters may consist of letters and numbers, and any combination of these, but cannot start with a number or a currency symbol.
    let makeKey len = String(Array.init len (fun _ -> chars.[rnd.Next(chars.Length - 1)]))

    let make (e : Expr) =
        let mutable parameters = List.Empty
       
        let addParam o (key : string) =
            let o = Serialization.fixTypes (o.GetType()) o
            parameters <- (key, o) :: parameters
            "$" + key
                
        let rec builder (e : Expr) =
            //printfn "%A" e
            match e with
            | SpecificCall <@@ (=) @@> (_, _, [ left; right ]) ->
                match left, right with
                | Var v, PropertyGet (None, pi, []) -> 
                    let o = 
                        QuotationEvaluator.EvaluateUntyped right
                        :?> IFSEntity
                        |> Serialization.serialize
                        |> box
                    
                    [ Choice1Of2 v.Name ; Choice1Of2 "=" ; Choice2Of2 (addParam o) ]

                | PropertyGet (None, pi, []), Var v -> 
                    let o = 
                        QuotationEvaluator.EvaluateUntyped right
                        :?> IFSEntity
                        |> Serialization.serialize
                        |> box

                    [ Choice2Of2 (addParam o) ; Choice1Of2 "=" ; Choice1Of2 v.Name ]

                | _ -> builder left @  [ Choice1Of2 "=" ] @ builder right
            | SpecificCall <@@ (<) @@> (_, _, [ left; right ]) -> builder left @ [ Choice1Of2 "<" ] @ builder right
            | SpecificCall <@@ (<=) @@> (_, _, [ left; right ]) -> builder left @ [ Choice1Of2 "<=" ] @ builder right
            | SpecificCall <@@ (>) @@> (_, _, [ left; right ]) -> builder left @ [ Choice1Of2 ">" ] @ builder right
            | SpecificCall <@@ (>=) @@> (_, _, [ left; right ]) -> builder left @ [ Choice1Of2 ">=" ] @ builder right
            | SpecificCall <@@ (<>) @@> (_, _, [ left; right ]) -> builder left @ [ Choice1Of2 "<>" ] @ builder right
            | IfThenElse (left, right, Value (o, t)) -> builder left @ [ Choice1Of2 "AND" ] @ builder right // Value(false) for AND
            | IfThenElse (left, Value (o, t), right) -> builder left @ [ Choice1Of2 "OR" ] @ builder right // Value(true) for OR
            | NewTuple exprs -> // Here for set statement - maybe throw on WHERE?
                exprs 
                |> List.collect (fun e -> builder e @ [ Choice1Of2 ", " ]) 
                
            | NewUnionCase (ui, [singleCase]) -> builder singleCase
            | NewUnionCase (ui, _) when ui.Name = "Cons" || ui.Name = "Empty" -> [ QuotationEvaluator.EvaluateUntyped e |> addParam |> Choice2Of2 ]
            | NewArray (_, _) -> [ QuotationEvaluator.EvaluateUntyped e |> addParam |> Choice2Of2 ]
            | ValueWithName (o, t, s) -> invalidOp "WHERE/SET: Value with name matched - not yet written!"  //"MAKE ME NAME" + s TODO
            | Value (o, t) -> [ Choice2Of2 (addParam o) ] // Also matches ValueWithName
            | PropertyGet (Some (PropertyGet (Some e, pi, _)), _, _) -> builder e @  Choice1Of2 "." :: [ Choice1Of2 pi.Name ] // Deeper than single "." used for options .Value
            | PropertyGet (Some e, pi, _) -> builder e @  Choice1Of2 "." :: [ Choice1Of2 pi.Name ]
            | PropertyGet (None, pi, _) -> [ Choice1Of2 pi.Name ]
            | Var v -> [ Choice1Of2 v.Name ]
            | Let (_, _, e2) -> builder e2
            | Lambda (_, e) -> builder e
            | _ -> 
                sprintf "Un matched in WHERE/SET statement: %A" e
                |> invalidOp
    
        builder e, parameters

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
            | ReturnContination _ -> invalidOp "Only 1 x RETURN statement is allowed in a Cypher query."
            | Empty -> ReturnContination v
        member this.Value =
            match this with
            | ReturnContination v -> v
            | Empty -> invalidOp "A Cypher query must contain a RETURN clause" 

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

            let buildQry (e : Expr<Query<'T>>) =
                //let mutable count = 0
                let mutable returnStatement : ReturnContination<'T> = Empty
                let rec queryBuilder (state : CypherStep list) (e : Expr) =
                    //count <- count + 1
                    match e with
                    | SpecificCall <@ cypher.Yield @> _ -> state
                    | SpecificCall <@ cypher.For @> (exp, types, [ stepAbove; thisStep ]) -> state
                    | Let (_ , _, right) -> queryBuilder state right
                    | SpecificCall <@ cypher.MATCH @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.MATCH stepAbove thisStep  
                        let statement = MatchClause.make thisStep
                        let newState = NonParameterized(MATCH, statement) :: state
                        queryBuilder newState stepAbove

                    // TODO: This can RETURN some nulls.. need to look further into that
                    | SpecificCall <@ cypher.OPTIONAL_MATCH @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.OPTIONAL_MATCH stepAbove thisStep  
                        let statement = MatchClause.make thisStep
                        let newState = NonParameterized(OPTIONAL_MATCH, statement) :: state
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.CREATE @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.CREATE stepAbove thisStep  
                        let statement = MatchClause.make thisStep
                        let newState = NonParameterized(CREATE, statement) :: state
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.MERGE @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.MERGE stepAbove thisStep  
                        let statement = MatchClause.make thisStep
                        let newState = NonParameterized(MERGE, statement) :: state
                        queryBuilder newState stepAbove
                    
                    | SpecificCall <@ cypher.WHERE @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.WHERE stepAbove thisStep  
                        let (statement, prms) = WhereAndSetStatement.make thisStep
                        let newState = Parameterized(WHERE, statement, prms) :: state
                        queryBuilder newState stepAbove
                    
                    | SpecificCall <@ cypher.SET @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.SET stepAbove thisStep  
                        let (statement, prms) = WhereAndSetStatement.make thisStep
                        let newState = Parameterized(SET, statement, prms) :: state
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