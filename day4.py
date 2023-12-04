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


# Brainstorming a not looping way to get answer
# but doesn't work
# max_wins=wins['wins'].max()
# lookback=(
# wins
# .select('Card_num', 'wins', lookback0=pl.lit(1),
#     **{f"lookback{x}": (
#     pl.when(pl.col('wins').shift(x)>=x)
#     .then(pl.lit(1))
#     .otherwise(pl.lit(0))) for x in range(1,1+max_wins)
#                      })
# )

ans_wins = wins
card_range = ans_wins["Card_num"].unique()
for card in card_range:
    # Add copies of cards
    cur_card_wins = ans_wins.filter(pl.col("Card_num") == card)["wins"][0]
    if cur_card_wins == 0:
        next
    card_copies = ans_wins.filter(
        pl.col("Card_num").is_between(card + 1, card + cur_card_wins)
    ).unique()
    ans_wins = pl.concat(
        [
            ans_wins,
            *[
                card_copies
                for _ in range(ans_wins.filter(pl.col("Card_num") == card).shape[0])
            ],
        ]
    )
part2_ans = ans_wins.shape[0]
## This takes about 16.5s
# 18846301

### Numba
import numba as nb
import numpy as np


@nb.guvectorize([(nb.int32[:], nb.int32[:])], "(n)->(n)", nopython=True)
def cum_cards(wins, res):
    res[:] = np.repeat(1, wins.shape[0])
    for card in range(wins.shape[0]):
        for i in range(wins[card]):
            true_i = card + i + 1
            res[true_i] = res[true_i] + res[card]


part2_nb_ans = wins.with_columns(haves=cum_cards(pl.col("wins").cast(pl.Int32)))[
    "haves"
].sum()
## This takes about 0s
# 18846301
