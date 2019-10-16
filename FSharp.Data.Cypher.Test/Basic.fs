namespace FSharp.Data.Cypher.Test.DELETE

open FSharp.Data.Cypher
open Xunit

module ``Can build`` = 

    [<Fact>]
    let ``Statement`` () =
        cypher {
            DELETE ()
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("DELETE null", q)

namespace FSharp.Data.Cypher.Test.DEATCH_DELETE

open FSharp.Data.Cypher
open Xunit

module ``Can build`` = 

    [<Fact>]
    let ``Statement`` () =
        cypher {
            DETACH_DELETE ()
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("DETACH DELETE null", q)

namespace FSharp.Data.Cypher.Test.ORDER_BY

open FSharp.Data.Cypher
open Xunit

module ``Can build`` = 

    [<Fact>]
    let ``Statement`` () =
        cypher {
            ORDER_BY ()
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("ORDER BY null", q)

namespace FSharp.Data.Cypher.Test.DESC

open FSharp.Data.Cypher
open Xunit

module ``Can build`` = 

    [<Fact>]
    let ``Statement`` () =
        cypher {
            DESC
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("DESC", q)

    [<Fact>]
    let ``Order by descending`` () =
        cypher {
            ORDER_BY 42
            DESC
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("ORDER BY 42 DESC", q)

namespace FSharp.Data.Cypher.Test.SKIP

open FSharp.Data.Cypher
open Xunit

module ``Can build`` = 

    [<Fact>]
    let ``Statement`` () =
        cypher {
            SKIP 42L
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("SKIP 42", q)

namespace FSharp.Data.Cypher.Test.LIMIT

open FSharp.Data.Cypher
open Xunit

module ``Can build`` = 

    [<Fact>]
    let ``Statement`` () =
        cypher {
            LIMIT 42L
        }
        |> Cypher.rawQuery
        |> fun q -> Assert.Equal("LIMIT 42", q)