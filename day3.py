import polars as pl

with open("day3_input.txt") as ff:
    intxt = ff.read()

df = pl.DataFrame({"a": intxt.split("\n")})

df = (
    df.with_columns(
        pl.col("a").str.split("").list.gather(pl.int_range(1, pl.count() + 1)),
        c=pl.int_ranges(0, pl.count()),
    )
    .with_row_count("r")
    .explode("a", "c")
)

nums = (
    df.with_columns(
        is_num=pl.col("a").str.extract("(\d)").is_not_null(),
        t=pl.col("a").str.extract("(\d)").is_not_null().rle_id().over("r"),
    )
    .group_by("t", "r", maintain_order=True)
    .agg(pl.col("a"), pl.col("c"), pl.col("is_num"))
    .select(
        "r",
        "c",
        pl.col("is_num").list.first(),
        pl.col("a").list.join(""),
        min_c=pl.col("c").list.min(),
        max_c=pl.col("c").list.max(),
    )
    .filter(pl.col("is_num"))
    .drop("is_num")
    .with_columns(pl.col("a").cast(pl.Int32()))
    .explode("c")
    .with_columns(pl.col("r").cast(pl.Int32), pl.col("c").cast(pl.Int32))
)

symbols = (
    df.filter((pl.col("a") != ".") & (pl.col("a").str.extract("(\d)").is_null()))
    .with_columns(
        r=pl.concat_list(pl.int_ranges(pl.col("r") - 1, pl.col("r") + 2)),
        c=pl.concat_list(pl.int_ranges(pl.col("c") - 1, pl.col("c") + 2)),
    )
    .explode("r")
    .explode("c")
    .with_columns(pl.col("r").cast(pl.Int32), pl.col("c").cast(pl.Int32))
)


part_one_ans = (
    symbols.join(nums, on=["r", "c"]).unique(["r", "min_c", "max_c"])["a_right"].sum()
)


### Part two

asterisks = (
    df.filter(pl.col("a") == "*")
    .with_columns(
        orig_r=pl.col("r"),
        orig_c=pl.col("c"),
        r=pl.concat_list(pl.int_ranges(pl.col("r") - 1, pl.col("r") + 2)),
        c=pl.concat_list(pl.int_ranges(pl.col("c") - 1, pl.col("c") + 2)),
    )
    .explode("r")
    .explode("c")
    .with_columns(pl.col("r").cast(pl.Int32), pl.col("c").cast(pl.Int32))
)

joined = asterisks.join(nums, on=["r", "c"]).unique(["r", "min_c", "max_c"])

part_two_ans = (
    joined.filter(pl.count().over(["orig_r", "orig_c"]) == 2)
    .sort(["orig_r", "orig_c"])
    .group_by(["orig_r", "orig_c"])
    .agg(pl.col("a_right").product())["a_right"]
    .sum()
)
