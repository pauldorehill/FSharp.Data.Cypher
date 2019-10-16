namespace FSharp.Data.Cypher

open System
open System.Collections
open Neo4j.Driver

type ReturnContination<'T> = Generic.IReadOnlyDictionary<string, obj> -> 'T
type ParameterList = (string * obj option) list

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
    let [<Literal>] DESC = "DESC"
    let [<Literal>] SKIP = "SKIP"
    let [<Literal>] LIMIT = "LIMIT"
    let [<Literal>] DELETE = "DELETE"
    let [<Literal>] DETACH_DELETE = "DETACH_DELETE"
    // TODO - here for write/read check completeness
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
    | DESC
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
        | DESC -> ClauseNames.DESC
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
    member this.IsRead = not (this.IsWrite)
         
module Clause =
    
    let isRead (clause : Clause) = clause.IsRead

    let isWrite (clause : Clause) = clause.IsWrite

type QueryResult<'T>(results : 'T [], summary : IResultSummary) =
    member _.Results = results
    member _.Summary = summary

module QueryResult =

    let results (qr : QueryResult<'T>) = qr.Results

    let summary (qr : QueryResult<'T>) = qr.Summary

type TransactionResult<'T>(results : 'T [], summary : IResultSummary, session : IAsyncSession, transaction : IAsyncTransaction) =
    member _.Results = results
    member _.Summary = summary
    member _.Session = session
    member _.Transaction = transaction
    member this.AsyncCommit() = 
        async {
            do! this.Transaction.CommitAsync() |> Async.AwaitTask
            do! this.Session.CloseAsync() |> Async.AwaitTask
            return QueryResult(this.Results, this.Summary)
        }
    member this.Commit() = this.AsyncCommit() |> Async.RunSynchronously
    member this.AsyncRollback() =
        async {
            do! this.Transaction.RollbackAsync() |> Async.AwaitTask
            do! this.Session.CloseAsync() |> Async.AwaitTask
            return ()
        }
    member this.Rollback() = this.AsyncRollback() |> Async.RunSynchronously

module TransactionResult =

    let results (tr : TransactionResult<'T>) = tr.Results
    
    let summary (tr : TransactionResult<'T>) = tr.Summary
    
    let transaction (tr : TransactionResult<'T>) = tr.Transaction

    let commit (tr : TransactionResult<'T>) = tr.Commit()
    
    let rollback (tr : TransactionResult<'T>) = tr.Rollback()

    let asyncCommit (tr : TransactionResult<'T>) = tr.AsyncCommit()
    
    let asyncRollback (tr : TransactionResult<'T>) = tr.AsyncRollback()

type Cypher<'T> internal (continuation , parameters, query, queryMultiline, rawQuery, rawQueryMultiline, isWrite) =
    member _.Continuation : ReturnContination<'T> option = continuation
    member _.Parameters : ParameterList = parameters
    member _.Query : string = query
    member _.QueryMultiline : string = queryMultiline
    member _.RawQuery : string = rawQuery 
    member _.RawQueryMultiline : string = rawQueryMultiline
    member _.IsWrite : bool = isWrite

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
    let private asyncRunTransaction (driver : IDriver) (map : 'T -> 'U) (cypher : Cypher<'T>) =
        async {
            let session = driver.AsyncSession()
            try
                let run (t : IAsyncTransaction) = t.RunAsync(cypher.Query, makeParameters cypher) 

                let! statementCursor = 
                    if cypher.IsWrite then session.WriteTransactionAsync run else session.ReadTransactionAsync run
                    |> Async.AwaitTask

                let! results = 
                    match cypher.Continuation with
                    | Some continuation -> statementCursor.ToListAsync(fun record -> continuation record.Values |> map) |> Async.AwaitTask
                    | None -> async.Return(ResizeArray())

                let! summary = statementCursor.SummaryAsync() |> Async.AwaitTask
            
                return QueryResult(Seq.toArray results, summary)

            finally
                session.CloseAsync()
                |> Async.AwaitTask
                |> Async.RunSynchronously 
                |> ignore 
        }

    let asyncRunMap driver map cypher = asyncRunTransaction driver map cypher

    let asyncRun driver cypher = asyncRunMap driver id cypher

    let runMap (driver : IDriver) map cypher = asyncRunMap driver map cypher |> Async.RunSynchronously

    let run (driver : IDriver) cypher = runMap driver id cypher

    let spoof (di : Generic.IReadOnlyDictionary<string, obj>) (cypher : Cypher<'T>) =
        cypher.Continuation
        |> Option.map (fun continuation -> continuation di)
        |> Option.defaultValue Unchecked.defaultof<'T>

    let rawQuery (cypher : Cypher<'T>) = cypher.RawQuery
    
    let query (cypher : Cypher<'T>) = cypher.Query

    /// Returns a TransactionResult - where the transation needs to be commited to the database or rolled back manually
    module Explicit =
    
        let private runTransaction (session : IAsyncSession) (map : 'T -> 'U) (cypher : Cypher<'T>) =
            async {
                let! transaction = session.BeginTransactionAsync() |> Async.AwaitTask

                let! statementCursor = transaction.RunAsync(cypher.Query, makeParameters cypher) |> Async.AwaitTask

                let! results = 
                    match cypher.Continuation with
                    | Some continuation -> 
                        statementCursor.ToListAsync(fun record -> continuation record.Values |> map) |> Async.AwaitTask
                    | None -> async.Return(ResizeArray())

                let! summary = statementCursor.SummaryAsync() |> Async.AwaitTask

                return TransactionResult(Array.ofSeq results, summary, session, transaction)
            }
        
        let asyncRunMap (driver : IDriver) map (cypher : Cypher<'T>) =
            let session = 
                if cypher.IsWrite 
                then 
                    driver.AsyncSession(fun sc ->
                        sc.DefaultAccessMode <- AccessMode.Write)
                else 
                    driver.AsyncSession(fun sc -> 
                        sc.DefaultAccessMode <- AccessMode.Read)
            try runTransaction session map cypher
            with e ->
                session.CloseAsync()
                |> Async.AwaitTask
                |> Async.RunSynchronously 
                |> ignore  
                raise e

        let asyncRun driver cypher = asyncRunMap driver id cypher
        
        let runMap driver map cypher = async.Return (runMap driver map cypher)
        
        let run (driver : IDriver) cypher = runMap driver id cypher