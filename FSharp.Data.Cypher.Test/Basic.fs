﻿namespace FSharp.Data.Cypher.Test.DELETE

open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    [<Fact>]
    let ``Statement`` () =
        cypher {
            DELETE ()
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("DELETE null", q)

namespace FSharp.Data.Cypher.Test.DETACH_DELETE

open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    [<Fact>]
    let ``Statement`` () =
        cypher {
            DETACH_DELETE ()
        }
        |> Cypher.query
        |> Query.raw
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
        |> Cypher.query
        |> Query.raw
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
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("DESC", q)

    [<Fact>]
    let ``Order by descending`` () =
        cypher {
            ORDER_BY 42
            DESC
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("ORDER BY 42 DESC", q)

namespace FSharp.Data.Cypher.Test.ASC

open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    [<Fact>]
    let ``Statement`` () =
        cypher {
            ASC
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("ASC", q)

    [<Fact>]
    let ``Order by descending`` () =
        cypher {
            ORDER_BY 42
            ASC
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("ORDER BY 42 ASC", q)

namespace FSharp.Data.Cypher.Test.SKIP

open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    [<Fact>]
    let ``Statement`` () =
        cypher {
            SKIP 42L
        }
        |> Cypher.query
        |> Query.raw
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
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("LIMIT 42", q)

namespace FSharp.Data.Cypher.Test.WITH

open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    [<Fact>]
    let ``Statement`` () =
        cypher {
            WITH 42L
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("WITH 42", q)

    [<Fact>]
    let ``Statement of tuples`` () =
        cypher {
            WITH (42L, "EMU", 42.1)
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("WITH 42, \"EMU\", 42.1", q)

namespace FSharp.Data.Cypher.Test.UNION

open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    [<Fact>]
    let ``Statement`` () =
        cypher {
            UNION
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("UNION", q)

namespace FSharp.Data.Cypher.Test.UNION_ALL

open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    [<Fact>]
    let ``Statement`` () =
        cypher {
            UNION_ALL
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("UNION ALL", q)

namespace FSharp.Data.Cypher.Test.PROFILE

open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    [<Fact>]
    let ``Statement`` () =
        cypher {
            PROFILE
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("PROFILE", q)

namespace FSharp.Data.Cypher.Test.EXPLAIN

open FSharp.Data.Cypher
open Xunit

module ``Can build`` =

    [<Fact>]
    let ``Statement`` () =
        cypher {
            EXPLAIN
        }
        |> Cypher.query
        |> Query.raw
        |> fun q -> Assert.Equal("EXPLAIN", q)
