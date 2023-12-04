import polars as pl


# sample="""Game 1: 3 blue, 4 red; 1 red, 2 green, 6 blue; 2 green
# Game 2: 1 blue, 2 green; 3 green, 4 blue, 1 red; 1 green, 1 blue
# Game 3: 8 green, 6 blue, 20 red; 5 blue, 4 red, 13 green; 5 green, 1 red
# Game 4: 1 green, 3 red, 6 blue; 3 green, 6 red; 3 green, 15 blue, 14 red
# Game 5: 6 red, 1 blue, 3 green; 2 blue, 1 red, 2 green"""

# df=pl.DataFrame({'a':sample.split('\n')})

with open("day2_input.txt") as ff:
    intxt = ff.read()
df = pl.DataFrame({"a": intxt.split("\n")})

df = (
    df.with_columns(
        cols=pl.col("a")
        .str.extract_groups("^Game\s+(\d+): (.+)")
        .struct.rename_fields(["Game_num", "else"])
    )
    .unnest("cols")
    .with_columns(
        pl.col("Game_num").cast(pl.Int32),
        rounds=pl.col("else").str.replace_all(" ", "").str.split(";"),
    )
    .explode("rounds")
    .with_columns(
        pl.col("rounds")
        .str.extract(f"(\d?+{color})")
        .str.replace(color, "")
        .cast(pl.Int32)
        .fill_null(0)
        .alias(color)
        for color in ["red", "blue", "green"]
    )
)

impossible = (
    df.filter((pl.col("red") > 12) | (pl.col("blue") > 14) | (pl.col("green") > 13))
)["Game_num"].unique()

part_one_ans = (
    df.filter(~pl.col("Game_num").is_in(impossible))["Game_num"].unique().sum()
)

## Part Two

part_two_ans = (
    df.group_by("Game_num")
    .agg(pl.col("red", "blue", "green").max())
    .select((pl.col("red") * pl.col("blue") * pl.col("green")).sum())
    .item()
)
