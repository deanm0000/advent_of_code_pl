import polars as pl

### Part One

# The engineer explains that an engine part seems to be missing from the engine,
# but nobody can figure out which one. If you can add up all the part numbers in
# the engine schematic, it should be easy to work out which part is missing.

# The engine schematic (your puzzle input) consists of a visual representation
# of the engine. There are lots of numbers and symbols you don't really understand,
# but apparently any number adjacent to a symbol, even diagonally, is a "part number"
# and should be included in your sum. (Periods (.) do not count as a symbol.)


with open("day3_input.txt") as ff:
    intxt = ff.read()

df = pl.DataFrame({"a": intxt.split("\n")})


# Split the rows into each individual character and map it by its row/column position
df = (
    df.with_columns(
        pl.col("a").str.split("").list.gather(pl.int_range(1, pl.count() + 1)),
        c=pl.int_ranges(0, pl.count()),
    )
    .with_row_count("r")
    .explode("a", "c")
)


# Make DF of just the numbers with begin and end column position within each row but
# also keep an entry for every column position the number takes up
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

# Make a DF of just the non . symbols and then copy each one's r/c position to be
# r=r-1, r, r+1 and c=c-1, c, c+1 to capture the diagonals
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

# Join the two DFs by their r/c position and then only keep unique by r, min_c, max_c. The sum of the
# a_right column is the final answer
part_one_ans = (
    symbols.join(nums, on=["r", "c"]).unique(["r", "min_c", "max_c"])["a_right"].sum()
)


### Part two

# The missing part wasn't the only issue - one of the gears in the engine is wrong. A gear is any *
# symbol that is adjacent to exactly two part numbers. Its gear ratio is the result of multiplying
# those two numbers together.

# This time, you need to find the gear ratio of every gear and add them all up so that the engineer
# can figure out which gear needs to be replaced.


# Make a DF of just * the same way as symbols above. This time maintain the original position
# of each asterisk so it can be grouped by later.
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

# Just like in part 1, join by r/c and take unique but then another step follows
joined = asterisks.join(nums, on=["r", "c"]).unique(["r", "min_c", "max_c"])

# Filter the join by the count for each symbol (denoted by original positions) and
# do grouping product and sum the series for final answer
part_two_ans = (
    joined.filter(pl.count().over(["orig_r", "orig_c"]) == 2)
    .sort(["orig_r", "orig_c"])
    .group_by(["orig_r", "orig_c"])
    .agg(pl.col("a_right").product())["a_right"]
    .sum()
)
