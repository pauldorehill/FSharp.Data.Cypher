namespace FSharp.Data.Cypher.Test.Return

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
            |> Cypher.queryNonParameterized
            |> fun r -> Assert.Equal("RETURN true", r)
    
        [<Fact>]
        let ``Bool false`` () =
            cypher {
                RETURN false
            }
            |> Cypher.queryNonParameterized
            |> fun r -> Assert.Equal("RETURN false", r)
        
        [<Fact>]
        let ``int32`` () =
            cypher {
                RETURN 5
            }
            |> Cypher.queryNonParameterized
            |> fun r -> Assert.Equal("RETURN 5", r)
    
        [<Fact>]
        let ``int64`` () =
            cypher {
                RETURN 5L
            }
            |> Cypher.queryNonParameterized
            |> fun r -> Assert.Equal("RETURN 5", r)
        
        [<Fact>]
        let ``float`` () =
            cypher {
                RETURN 5.5
            }
            |> Cypher.queryNonParameterized
            |> fun r -> Assert.Equal("RETURN 5.5", r)
    
        [<Fact>]
        let ``string`` () =
            cypher {
                RETURN "EMU"
            }
            |> Cypher.queryNonParameterized
            |> fun r -> Assert.Equal("RETURN \"EMU\"", r)

module ``Deserialize`` =

    module ``Primative Types`` =

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

        module ``Spoofed Results`` =

            let [<Fact>] ``Bool true`` () = fBoolTrue f 
            let [<Fact>] ``Bool false`` () = fBoolFalse f
            let [<Fact>] ``int32`` () = fInt32 f
            let [<Fact>] ``int64`` () = fInt64 f
            let [<Fact>] ``float`` () = fFloat f
            let [<Fact>] ``string`` () = fString f
            let [<Fact>] ``tuple of all`` () = fTupleOfAll f
    
        module ``Live Graph Results`` =

            let f x =
                Cypher.run LocalGraph.Driver x
                |> QueryResult.results
                |> Seq.head
        
            let [<Fact>] ``Bool true`` () = fBoolTrue f
            let [<Fact>] ``Bool false`` () = fBoolFalse f
            let [<Fact>] ``int32`` () = fInt32 f
            let [<Fact>] ``int64`` () = fInt64 f
            let [<Fact>] ``float`` () = fFloat f
            let [<Fact>] ``string`` () = fString f
            let [<Fact>] ``tuple of all`` () = fTupleOfAll f

    module ``All allowed Types on a Record`` =

        let spoofDic =  
            [ "allAllowedSome", box ``All Allowed``.allAllowedSome
              "allAllowedNone", box ``All Allowed``.allAllowedNone 
              "nonCollections", box ``All Allowed``.nonCollections ]
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