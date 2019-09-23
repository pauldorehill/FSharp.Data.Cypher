namespace FSharp.Data.Cypher

open System
open System.Collections
open FSharp.Quotations
open Neo4j.Driver.V1

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
        let makeParms (c : Clause) (stepCount : int) (prms : Choice<string,string -> (string * obj option)> list) =
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

    /// Returns a TransactionResult - where the transation needs to be commited to the database or rolled back manually
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