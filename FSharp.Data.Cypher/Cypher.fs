namespace FSharp.Data.Cypher

open System
open System.Reflection
open System.Collections
open FSharp.Reflection
open FSharp.Quotations
open FSharp.Quotations.Patterns
open FSharp.Quotations.DerivedPatterns
open FSharp.Quotations.ExprShape
open Neo4j.Driver.V1
open FSharp.Data.Cypher

type Query<'T> = NA

exception InvalidMatchClause of string
exception InvalidReturnClause of string

type Clause = 
    | Clause of string
    member this.Value = match this with | Clause x -> x
    override this.ToString() = this.Value.Replace("_", " ")

type QueryStep(clause : Clause, statement : string, ?parameters : (string * obj) list ) =
    member __.Clause = clause
    member __.Statement = statement
    member __.Parameters = parameters
    member __.CompleteStep = string clause + " " + statement

type Cypher<'T>(querySteps : QueryStep list, continuation : Generic.IReadOnlyDictionary<string, obj> -> 'T) =
    let ChooseParams =
        querySteps
        |> List.choose (fun x -> x.Parameters)
        |> List.concat
    
    let MakeQuery sep =
        querySteps
        |> List.map (fun x -> x.CompleteStep)
        |> String.concat sep

    member __.QuerySteps = querySteps

    member __.Continuation = continuation

    // Neo4j Driver is not happy unless this is Dictionary - doesn't like some F# collections even though implement IDictionary
    // it will give Neo4j.Driver.V1.ServiceUnavailableException: Unexpected end of stream, read returned 0
    member __.Parameters =
        ChooseParams
        |> dict
        |> Generic.Dictionary

    member __.Query = MakeQuery " "

    member __.QueryMultiline = MakeQuery Environment.NewLine

    member this.QueryNonParameterized = // TODO : should capture the raw statement so no need to do this
        (this.Query, ChooseParams)
        ||> List.fold (fun state (k, v) -> 
            let v = if v.GetType()= typeof<string> then string v |> sprintf "\"%s\"" else string v
            state.Replace("$" + k, v)) 
        
type CypherResult<'T>(results : 'T seq, summary : IResultSummary) =
    member __.Results = results
    member __.Summary = summary

module CypherResult =

    let results (cr : CypherResult<'T>) = cr.Results
    let summary (cr : CypherResult<'T>) = cr.Summary

module Cypher =

    let private runTransaction (cypher : Cypher<'T>) (driver : IDriver) (accessMode : AccessMode) (map : 'T -> 'U) =
        use session = driver.Session accessMode
        try
            let sr = session.BeginTransaction().Run(cypher.Query, cypher.Parameters)
            let results = Seq.map (fun (record : IRecord) -> cypher.Continuation record.Values |> map) sr |> Seq.toArray
            CypherResult(results, sr.Summary)

        finally
            session.CloseAsync() |> ignore 

    let queryNonParameterized (cypher : Cypher<'T>) = cypher.QueryNonParameterized
    
    let writeMap (driver : IDriver) map (cypher : Cypher<'T>) = runTransaction cypher driver AccessMode.Write map 
    let writeMapAsync (driver : IDriver) map (cypher : Cypher<'T>) = async { return writeMap driver map cypher }

    let write (driver : IDriver) (cypher : Cypher<'T>) = writeMap driver id cypher
    let writeAsync (driver : IDriver) (cypher : Cypher<'T>) = async { return writeMap driver id cypher }

    let readMap (driver : IDriver) map (cypher : Cypher<'T>) = runTransaction cypher driver AccessMode.Read map
    let readMapAsync (driver : IDriver) map (cypher : Cypher<'T>) = async { return readMap driver map cypher }

    let read (driver : IDriver) (cypher : Cypher<'T>) = readMap driver id cypher
    let readAsync (driver : IDriver) (cypher : Cypher<'T>) = async { return read driver cypher }

    let spoof (di : Generic.IReadOnlyDictionary<string, obj>) (cypher : Cypher<'T>) = cypher.Continuation di

module private Loggers =

    let logger (stepCount : int) (specifCallName : string) (stepAbove : Expr) (thisStep : Expr) =
        printfn "Step: %i. Step Name: %s" stepCount specifCallName
        printfn "This step:"
        printfn "%A"  thisStep
        printfn "Steps above:" 
        printfn "%A" stepAbove

module Exception =
    
    let invalidMatchClause e = InvalidMatchClause e |> raise

    let invalidReturnClause e = InvalidReturnClause e |> raise

module private MatchClause =
    
    open Exception

    let [<Literal>] private IFSNode = "IFSNode"
    let [<Literal>] private IFSRelationship = "IFSRelationship"

    let makeLabels expr typ name =
        let t = Evaluator.QuotationEvaluator.EvaluateUntyped expr
        if typ = typeof<IFSNode> || Deserialization.hasInterface typ IFSNode
        then 
            (t :?> IFSNode).Labels
            |> Option.map (fun xs ->
                xs
                |> List.map Label.make
                |> String.concat "")
            |> Option.defaultValue ""
            |> sprintf "(%s%s)" name

        elif typ = typeof<IFSRelationship> || Deserialization.hasInterface typ IFSRelationship
        then
            (t :?> IFSRelationship).Label
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
            | Coerce (PropertyGet (_, pi, _), typ) -> makeLabels expr typ pi.Name
            | Coerce (ex, _) -> inner ex
            | Let (_, _, e2) -> inner e2
            | TupleGet (exp, _) -> inner exp
            | Var v -> 
                Deserialization.createNullRecordOrClass v.Type
                |> fun o -> makeLabels <@ o @> v.Type v.Name
            | _ -> 
                expr
                |> sprintf "Unable to build ascii statement: %A"
                |> invalidMatchClause
    
        inner expr

    let make (expr : Expr) =
        match expr with
        | Lambda (v, exp) -> buildAscii exp
        | _ -> 
            expr
            |> sprintf "Step was not a Lambda as expected : %A"
            |> invalidMatchClause
 
module private ClauseHelpers =  
    
    let makePropertyKey (v : Var) (pi : PropertyInfo) = v.Name + "." + pi.Name

    let fixStringValue typ o = if typ = typeof<string> then string o |> sprintf "\"%s\"" else string o

    let extractStatement (exp : Expr) =
        match exp with
        | Value (o, typ) -> fixStringValue typ o
        | Var v -> v.Name // string needs to be escaped
        | PropertyGet (Some (Var v), pi, _) -> makePropertyKey v pi
        // When variable is outside the builder
        | PropertyGet (None, pi, _) -> pi.Name
        | _ ->
            exp
            |> sprintf "Trying to extract statement but couldn't match expression: %A"
            |> invalidOp

module WhereAndSetStatement =

    let private rnd = Random()
    let private chars = "ABCDEFGHIJKLMNOPQRSTUVXYZabcdefghijklmnopqrstuvxyz".ToCharArray()

    let makeKey len = String(Array.init len (fun _ -> chars.[rnd.Next(chars.Length - 1)]))

    let make (e : Expr) =
        let mutable parameters = List.Empty
    
        let addParam o =
            let key = makeKey 15
            parameters <- (key, o) :: parameters
            "$" + key
                
        let rec builder (e : Expr) =
            //printfn "%A" e
            match e with
            | SpecificCall <@@ (=) @@> (_, _, [ left; right ]) -> 
                match left, right with
                | PropertyGet (None, piL, _), PropertyGet (None, piR, _) -> // Case when its a type on both sides, need to serialize the type
                    let o = invalidOp "Need to write type serializer" // TODO
                    piL.Name + " = " + addParam o
                | _ -> builder left + " = " + builder right
            | SpecificCall <@@ (<) @@> (_, _, [ left; right ]) -> builder left + " < " + builder right
            | SpecificCall <@@ (<=) @@> (_, _, [ left; right ]) -> builder left + " <= " + builder right
            | SpecificCall <@@ (>) @@> (_, _, [ left; right ]) -> builder left + " > " + builder right
            | SpecificCall <@@ (>=) @@> (_, _, [ left; right ]) -> builder left + " >= " + builder right
            | SpecificCall <@@ (<>) @@> (_, _, [ left; right ]) -> builder left + " <> " + builder right
            | IfThenElse (left, right, Value (o, t)) -> builder left + " AND " + builder right // Value(false) for AND
            | IfThenElse (left, Value (o, t), right) -> builder left + " OR " + builder right // Value(true) for OR

            | NewTuple exprs -> // Here for set statement - maybe throw on WHERE?
                List.map builder exprs
                |> String.concat ", "

            | NewUnionCase (ui, [singleCase]) -> builder singleCase
            | Value (o, t) -> addParam o
            | PropertyGet (Some (PropertyGet (Some e, pi, _)), _, _) -> builder e + "." + pi.Name // Deeper than single "." used for options .Value
            | PropertyGet (Some e, pi, _) -> builder e + "." + pi.Name
            | PropertyGet (None, pi, _) -> pi.Name
            | Var v -> v.Name
            | Let (_, _, e2) -> builder e2
            | Lambda (_, e) -> builder e
            | _ -> 
                sprintf "Un matched in WHERE/SET statement: %A" e
                |> invalidOp
    
        builder e, parameters

module private ReturnClause =

    open ClauseHelpers
    open Exception

    let makeNewType (var : Var) (di : Generic.IReadOnlyDictionary<string, obj>) =
        di.[var.Name] 
        :?> IEntity
        |> Deserialization.deserialize var.Type
        |> Deserialization.createRecordOrClass var.Type

    let extractValue (di : Generic.IReadOnlyDictionary<string, obj>) (exp : Expr) =
        match exp with
        | Value (o, typ) ->
            let key = fixStringValue typ o
            let o = Deserialization.fixTypes key typ di.[key]
            Expr.Value(o, typ)

        | Var v -> 
            // Need to give the compiler some type info for final cast - so return as a value
            // This may be a bit of a hack, but it works!
            Expr.Value(makeNewType v di, v.Type)

        | PropertyGet (Some (Var v), pi, _) ->
            let key = makePropertyKey v pi
            let o = Deserialization.fixTypes pi.Name pi.PropertyType di.[key]
            Expr.Value(o, pi.PropertyType)

        | _ ->
            exp
            |> sprintf "Trying to extract values but couldn't match expression: %A"
            |> invalidReturnClause

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
                |> sprintf "Unrecognized expression: %A"
                |> invalidReturnClause

        match e with
        | Lambda (_, exp) ->
            inner exp
            |> fun (statement, continuation) ->
                let result di =
                    continuation di
                    //|> Linq.RuntimeHelpers.LeafExpressionConverter.EvaluateQuotation
                    |> Evaluator.QuotationEvaluator.EvaluateUntyped :?> 'T
                        
                statement, result
        | _ -> 
            e
            |> sprintf "Was not a Lambda : %A"
            |> invalidReturnClause

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

module private Clause =
    
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

[<AutoOpen>]
module CypherBuilder =
    // Initial help came from this great article by Thomas Petricek
    // http://tomasp.net/blog/2015/query-translation/
    // Other helpful articles
    // https://stackoverflow.com/questions/23122639/how-do-i-write-a-computation-expression-builder-that-accumulates-a-value-and-als
    // https://stackoverflow.com/questions/14110532/extended-computation-expressions-without-for-in-do
    open Clause
    open Loggers

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
        [<CustomOperation(MATCH, MaintainsVariableSpace = true)>]
        member __.MATCH(source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSNode) : Query<'T> = NA
        
        [<CustomOperation(OPTIONAL_MATCH, MaintainsVariableSpace = true)>]
        member __.OPTIONAL_MATCH(source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSNode) : Query<'T> = NA
        
        [<CustomOperation(CREATE, MaintainsVariableSpace = true)>]
        member __.CREATE(source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSNode) : Query<'T> = NA
        
        [<CustomOperation(MERGE, MaintainsVariableSpace = true)>]
        member __.MERGE(source : Query<'T>, [<ProjectionParameter>] f : 'T -> #IFSNode) : Query<'T> = NA
        
        [<CustomOperation(WHERE, MaintainsVariableSpace = true)>]
        member __.WHERE(source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'T> = NA
        
        [<CustomOperation(SET, MaintainsVariableSpace = true)>]
        member __.SET(source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'T> = NA

        [<CustomOperation(RETURN, MaintainsVariableSpace = true)>]
        member __.RETURN(source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'R> = NA
        
        [<CustomOperation(RETURN_DISTINCT, MaintainsVariableSpace = true)>]
        member __.RETURN_DISTINCT(source : Query<'T>, [<ProjectionParameter>] f : 'T -> 'R) : Query<'R> = NA
        
        // Can't get this to work with intellisense
        // Might need to change the type to have 2 x generics so can carry the final type
        [<CustomOperation(ORDER_BY, MaintainsVariableSpace = true)>]
        member __.ORDER_BY(source : Query<'T>, [<ProjectionParameter>] f) : Query<'T> = NA
        
        [<CustomOperation(SKIP, MaintainsVariableSpace = true)>]
        member __.SKIP(source : Query<'T>, f : int64) : Query<'T> = NA
        
        [<CustomOperation(LIMIT, MaintainsVariableSpace = true)>]
        member __.LIMIT(source : Query<'T>, f : int64) : Query<'T> = NA

        member __.Quote(query : Expr<Query<'T>>) = NA

        member cypher.Run(e : Expr<Query<'T>>) = 

            let buildQry (e : Expr<Query<'T>>) =
                //let mutable count = 0
                let mutable returnStatement : ReturnContination<'T> = Empty
                let rec queryBuilder (state : QueryStep list) (e : Expr) =
                    //count <- count + 1
                    match e with
                    | SpecificCall <@ cypher.Yield @> _ -> state
                    | SpecificCall <@ cypher.For @> (exp, types, [ stepAbove; thisStep ]) -> state
                    | SpecificCall <@ cypher.MATCH @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.MATCH stepAbove thisStep  
                        let statement = MatchClause.make thisStep
                        let newState = QueryStep(Clause MATCH, statement) :: state
                        queryBuilder newState stepAbove

                    // TODO: This can RETURN some nulls.. need to look further into that
                    | SpecificCall <@ cypher.OPTIONAL_MATCH @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.OPTIONAL_MATCH stepAbove thisStep  
                        let statement = MatchClause.make thisStep
                        let newState = QueryStep(Clause OPTIONAL_MATCH, statement) :: state
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.CREATE @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.CREATE stepAbove thisStep  
                        let statement = MatchClause.make thisStep
                        let newState = QueryStep(Clause CREATE, statement) :: state
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.MERGE @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.MERGE stepAbove thisStep  
                        let statement = MatchClause.make thisStep
                        let newState = QueryStep(Clause MERGE, statement) :: state
                        queryBuilder newState stepAbove
                    
                    | SpecificCall <@ cypher.WHERE @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.WHERE stepAbove thisStep  
                        let (statement, prms) = WhereAndSetStatement.make thisStep
                        let newState = QueryStep(Clause WHERE, statement, prms) :: state
                        queryBuilder newState stepAbove
                    
                    | SpecificCall <@ cypher.SET @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.SET stepAbove thisStep  
                        let (statement, prms) = WhereAndSetStatement.make thisStep
                        let newState = QueryStep(Clause SET, statement, prms) :: state
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.RETURN @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.RETURN stepAbove thisStep 
                        let (statement, continuation) = ReturnClause.make<'T> thisStep
                        let newState = QueryStep(Clause RETURN, statement) :: state
                        returnStatement <- returnStatement.Update continuation
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.RETURN_DISTINCT @> (exp, types, [ stepAbove; thisStep ]) ->
                        //logger count Clause.RETURN_DISTINCT stepAbove thisStep 
                        let (statement, continuation) = ReturnClause.make<'T> thisStep
                        let newState = QueryStep(Clause RETURN_DISTINCT, statement) :: state
                        returnStatement <- returnStatement.Update continuation
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.ORDER_BY @> (exp, types, [ stepAbove; thisStep ]) ->
                        let statement = BasicClause.make thisStep
                        let newState = QueryStep(Clause ORDER_BY, statement) :: state
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.SKIP @> (exp, types, [ stepAbove; thisStep ]) ->
                        let statement = BasicClause.make thisStep
                        let newState = QueryStep(Clause SKIP, statement) :: state
                        queryBuilder newState stepAbove

                    | SpecificCall <@ cypher.LIMIT @> (exp, types, [ stepAbove; thisStep ]) ->
                        let statement = BasicClause.make thisStep
                        let newState = QueryStep(Clause LIMIT, statement) :: state
                        queryBuilder newState stepAbove

                    | _ -> 
                        e
                        |> sprintf "Un matched method when building Query: %A"
                        |> invalidOp 

                queryBuilder [] e, returnStatement.Value
            
            Cypher(buildQry e)

    let cypher = CypherBuilder()