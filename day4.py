import polars as pl

with open("day4_input.txt") as ff:
    intxt = ff.read()

df = pl.DataFrame({"a": intxt.split("\n")})

day4_ans = (
    df.with_columns(
        cols=pl.col("a").str.extract_groups("^Card\s+(\d+): (.+)")
        ## I tried getting all the groups in one go but couldn't get regex right
        .struct.rename_fields(["Card_num", "else"])
    )
    .unnest("cols")
    .with_columns(
        pl.col("else")
        .str.split("|")
        .list.to_struct()
        .struct.rename_fields(["winning", "have"])
    )
    .unnest("else")
    .with_columns(
        pl.col("winning", "have")
        .str.strip_chars()
        .str.replace_all("\s\s+", " ")
        .str.split(" ")
    )
    .with_columns(wins=pl.col("winning").list.set_intersection("have"))
    .with_columns(
        points=(
            pl.when(pl.col("wins").list.len() <= 1)
            .then(pl.col("wins").list.len())
            .otherwise(pl.lit(2).pow(pl.col("wins").list.len() - 1))
        )
    )["points"]
    .cast(pl.Int32())
    .sum()
)

## Part Two
wins = (
    df.with_columns(
        cols=pl.col("a").str.extract_groups("^Card\s+(\d+): (.+)")
        ## I tried getting all the groups in one go but couldn't get regex right
        .struct.rename_fields(["Card_num", "else"])
    )
    .unnest("cols")
    .with_columns(
        pl.col("Card_num").cast(pl.Int64),
        pl.col("else")
        .str.split("|")
        .list.to_struct()
        .struct.rename_fields(["winning", "have"]),
    )
    .unnest("else")
    .with_columns(
        pl.col("winning", "have")
        .str.strip_chars()
        .str.replace_all("\s\s+", " ")
        .str.split(" ")
    )
    .select("Card_num", wins=pl.col("winning").list.set_intersection("have").list.len())
)
# max_wins=wins['wins'].max()

# wins.select('Card_num', 'wins', lookback0=pl.lit(1),
#     **{f"lookback{x}": (
#     pl.when(pl.col('wins').shift(x)>=x)
#     .then(pl.lit(1))
#     .otherwise(pl.lit(0))) for x in range(1,1+max_wins)
#                      })


card_range = wins["Card_num"].unique()
for card in card_range:
    # Add copies of cards
    cur_card_wins = wins.filter(pl.col("Card_num") == card)["wins"][0]
    if cur_card_wins == 0:
        next
    card_copies = wins.filter(
        pl.col("Card_num").is_between(card + 1, card + cur_card_wins)
    ).unique()
    wins = pl.concat(
        [
            wins,
            *[
                card_copies
                for _ in range(wins.filter(pl.col("Card_num") == card).shape[0])
            ],
        ]
    )
part2_ans = wins.shape[0]
