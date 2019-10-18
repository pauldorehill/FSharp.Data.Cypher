namespace FSharp.Data.Cypher.Test.WHERE

open System
open FSharp.Data.Cypher
open Xunit

module ``Allowed operators`` =
    
    [<Fact>]
    let ``Equals =`` () =

        cypher {
            WHERE (42 = 42)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("WHERE 42 = 42", q)
    
    [<Fact>]
    let ``Less than <`` () =

        cypher {
            WHERE (42 < 42)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("WHERE 42 < 42", q)
    
    [<Fact>]
    let ``Less than or Equals <=`` () =

        cypher {
            WHERE (42 <= 42)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("WHERE 42 <= 42", q)
    
    [<Fact>]
    let ``Greater than >`` () =

        cypher {
            WHERE (42 > 42)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("WHERE 42 > 42", q)
    
    [<Fact>]
    let ``Greater than or Equals >=`` () =

        cypher {
            WHERE (42 >= 42)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("WHERE 42 >= 42", q)
    
    [<Fact>]
    let ``Not Equal <>`` () =

        cypher {
            WHERE (42 <> 42)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("WHERE 42 <> 42", q)
        
    [<Fact>]
    let ``Or`` () =

        cypher {
            WHERE (42 <> 42 || 42 <> 42)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("WHERE 42 <> 42 OR 42 <> 42", q)
    
    [<Fact>]
    let ``And`` () =
        cypher {
            WHERE (42 <> 42 && 42 <> 42)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("WHERE 42 <> 42 AND 42 <> 42", q)

module ``Option types`` =
    
    [<Fact>]
    let ``Some value`` () =

        cypher {
            WHERE (Some 42 = Some 42)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("WHERE 42 = 42", q)
    
    [<Fact>]
    let ``None value`` () =

        cypher {
            WHERE (None = None)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("WHERE null = null", q)
        
    [<Fact>]
    let ``Some None`` () =

        cypher {
            WHERE (Some 42 = None)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("WHERE 42 = null", q)

namespace FSharp.Data.Cypher.Test.SET

open System
open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    [<Fact>]
    let ``Basic statement`` () =
        cypher {
            SET 42
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("SET 42", q)
    
    [<Fact>]
    let ``Tupled statement`` () =
        cypher {
            SET (42, 42, 42)
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("SET 42, 42, 42", q)

namespace FSharp.Data.Cypher.Test.ON_CREATE_SET

open System
open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    [<Fact>]
    let ``Basic statement`` () =
        cypher {
            ON_CREATE_SET 42
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("ON CREATE SET 42", q)

namespace FSharp.Data.Cypher.Test.ON_MATCH_SET

open System
open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    [<Fact>]
    let ``Basic statement`` () =
        cypher {
            ON_MATCH_SET 42
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("ON MATCH SET 42", q)
   
