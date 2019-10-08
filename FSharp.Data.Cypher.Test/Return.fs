namespace FSharp.Data.Cypher.Test.Return

open Neo4j.Driver.V1
open System.Collections
open FSharp.Data.Cypher
open FSharp.Data.Cypher.Test
open Xunit

module ``Query Building`` =

    module ``Primtive Types`` =

        [<Fact>]
        let ``Bool true`` () =
            cypher {
                RETURN true
            }
            |> Cypher.rawQuery
            |> fun r -> Assert.Equal("RETURN true", r)
    
        [<Fact>]
        let ``Bool false`` () =
            cypher {
                RETURN false
            }
            |> Cypher.rawQuery
            |> fun r -> Assert.Equal("RETURN false", r)
        
        [<Fact>]
        let ``int32`` () =
            cypher {
                RETURN 5
            }
            |> Cypher.rawQuery
            |> fun r -> Assert.Equal("RETURN 5", r)
    
        [<Fact>]
        let ``int64`` () =
            cypher {
                RETURN 5L
            }
            |> Cypher.rawQuery
            |> fun r -> Assert.Equal("RETURN 5", r)
        
        [<Fact>]
        let ``float`` () =
            cypher {
                RETURN 5.5
            }
            |> Cypher.rawQuery
            |> fun r -> Assert.Equal("RETURN 5.5", r)
    
        [<Fact>]
        let ``string`` () =
            cypher {
                RETURN "EMU"
            }
            |> Cypher.rawQuery
            |> fun r -> Assert.Equal("RETURN \"EMU\"", r)

module ``Deserialize: Spoofed Results`` =

    let spoofDic =  
        [ "true", box true
          "false", box false
          "5", box 5L // DB returns int64
          "7", box 7L
          "5.5", box 5.5
          "\"EMU\"", box "EMU" ]
        |> Map.ofList
        |> Generic.Dictionary

    let fBoolTrue f =
        cypher {
            RETURN true
        }
        |> f
        |> fun r -> Assert.Equal(true, r)
    
    let fBoolFalse f =
        cypher {
            RETURN false
        }
        |> f
        |> fun r -> Assert.Equal(false, r)
        
    let fInt32 f =
        cypher {
            RETURN 5
        }
        |> f
        |> fun r -> Assert.Equal(5, r)

    let fInt64 f =
        cypher {
            RETURN 7L
        }
        |> f
        |> fun r -> Assert.Equal(7L, r)

    let fFloat f =
        cypher {
            RETURN 5.5
        }
        |> f
        |> fun r -> Assert.Equal(5.5, r)

    let fString f =
        cypher {
            RETURN "EMU"
        }
        |> f
        |> fun r -> Assert.Equal("EMU", r)

    let fTupleOfAll f = 
        cypher {
            RETURN (true, false, 5, 7L, 5.5, "EMU")
        }
        |> f
        |> fun r -> Assert.Equal((true, false, 5, 7L, 5.5, "EMU"), r)
    
    let f x = Cypher.spoof spoofDic x


    let [<Fact>] ``Bool true`` () = fBoolTrue f 
    let [<Fact>] ``Bool false`` () = fBoolFalse f
    let [<Fact>] ``int32`` () = fInt32 f
    let [<Fact>] ``int64`` () = fInt64 f
    let [<Fact>] ``float`` () = fFloat f
    let [<Fact>] ``string`` () = fString f
    let [<Fact>] ``tuple of all`` () = fTupleOfAll f
    
module ``All allowed Types on a Record`` =

    type AllAllowed =
        { string : string
          stringOption : string option 
          stringSeq : string seq 
          stringList : string List 
          stringArray : string array 
          stringSet : string Set 
    
          int64 : int64
          int64Option : int64 option 
          int64Seq : int64 seq 
          int64List : int64 List 
          int64Array : int64 array 
          int64Set : int64 Set 
    
          int32 : int32
          int32Option : int32 option 
          int32Seq : int32 seq 
          int32List : int32 List 
          int32Array : int32 array 
          int32Set : int32 Set 
    
          float : float
          floatOption : float option 
          floatSeq : float seq 
          floatList : float List 
          floatArray : float array 
          floatSet : float Set 
    
          bool : bool
          boolOption : bool option 
          boolSeq : bool seq 
          boolList : bool List 
          boolArray : bool array 
          boolSet : bool Set }
        interface IFSNode

    let makeToReturnType xs =
        xs
        |> Map.ofList
        |> Generic.Dictionary
        |> fun xs ->
            { new IEntity with 
                member _.Id = invalidOp "Fake Entity."
                member this.get_Item (key : string) : obj = this.Properties.Item key
                member _.Properties = xs :> Generic.IReadOnlyDictionary<string,obj> }

    let baseList =
        [ "string", box "EMU"
          "stringOption", box "EMU"
          "stringSeq", box ([ box "EMU" ] |> Generic.List)
          "stringList", box ([ box "EMU" ] |> Generic.List)
          "stringArray", box ([ box "EMU" ] |> Generic.List)
          "stringSet", box ([ box "EMU" ] |> Generic.List)
          "int64", box 5L
          "int64Option", box 5L
          "int64Seq", box ([ box 5L ] |> Generic.List)
          "int64List", box ([ box 5L ] |> Generic.List)
          "int64Array", box ([ box 5L ] |> Generic.List)
          "int64Set", box ([ box 5L ] |> Generic.List)
          "int32", box 5L
          "int32Option", box 5L
          "int32Seq", box ([ box 5L ] |> Generic.List)
          "int32List", box ([ box 5L ] |> Generic.List)
          "int32Array", box ([ box 5L ] |> Generic.List)
          "int32Set", box ([ box 5L ] |> Generic.List)
          "float", box 5.5
          "floatOption", box 5.5
          "floatSeq", box ([ box 5.5 ] |> Generic.List)
          "floatList", box ([ box 5.5 ] |> Generic.List)
          "floatArray", box ([ box 5.5 ] |> Generic.List)
          "floatSet", box ([ box 5.5 ] |> Generic.List)
          "bool", box true
          "boolOption", box true
          "boolSeq", box ([ box true ] |> Generic.List)
          "boolList", box ([ box true ] |> Generic.List)
          "boolArray", box ([ box true ] |> Generic.List)
          "boolSet", box ([ box true ] |> Generic.List) ]

    let allAllowedSome = makeToReturnType baseList
    
    let allAllowedNone = 
        baseList 
        |> List.filter (fun (n, _) -> not(n.ToLower().Contains "option"))
        |> makeToReturnType

    let nonCollections =
        [ "string", box "EMU"
          "stringOption", box "EMU"
          "stringSeq", box "EMU"
          "stringList", box "EMU"
          "stringArray", box "EMU"
          "stringSet", box "EMU"
          "int64", box 5L
          "int64Option", box 5L
          "int64Seq", box 5L
          "int64List", box 5L
          "int64Array", box 5L
          "int64Set", box 5L
          "int32", box 5L
          "int32Option", box 5L
          "int32Seq", box 5L
          "int32List", box 5L
          "int32Array", box 5L
          "int32Set", box 5L
          "float", box 5.5
          "floatOption", box 5.5
          "floatSeq", box 5.5
          "floatList", box 5.5
          "floatArray", box 5.5
          "floatSet", box 5.5
          "bool", box false
          "boolOption", box false
          "boolSeq", box false
          "boolList", box false
          "boolArray", box false
          "boolSet", box false ]
        |> makeToReturnType

    let spoofDic =  
        [ "allAllowedSome", box allAllowedSome
          "allAllowedNone", box allAllowedNone 
          "nonCollections", box nonCollections ]
        |> Map.ofList
        |> Generic.Dictionary
    
    type Graph =
        static member AllAllowed : Query<AllAllowed> = NA

    [<Fact>]
    let ``Can do all when return Some`` ()=
        cypher {
            for allAllowedSome in Graph.AllAllowed do
            RETURN (allAllowedSome)
        }
        |> Cypher.spoof spoofDic
        |> Assert.IsType<AllAllowed>

    [<Fact>]
    let ``Can do all when return None`` ()=
        cypher {
            for allAllowedNone in Graph.AllAllowed do
            RETURN (allAllowedNone)
        }
        |> Cypher.spoof spoofDic
        |> Assert.IsType<AllAllowed>

    [<Fact>]
    let ``Can do all when collections are single items`` ()=
        cypher {
            for nonCollections in Graph.AllAllowed do
            RETURN (nonCollections)
        }
        |> Cypher.spoof spoofDic
        |> Assert.IsType<AllAllowed>