import polars as pl

# sample = """seeds: 79 14 55 13

# seed-to-soil map:
# 50 98 2
# 52 50 48

# soil-to-fertilizer map:
# 0 15 37
# 37 52 2
# 39 0 15

# fertilizer-to-water map:
# 49 53 8
# 0 11 42
# 42 0 7
# 57 7 4

# water-to-light map:
# 88 18 7
# 18 25 70

# light-to-temperature map:
# 45 77 23
# 81 45 19
# 68 64 13

# temperature-to-humidity map:
# 0 69 1
# 1 0 69

# humidity-to-location map:
# 60 56 37
# 56 93 4"""

# df = pl.DataFrame({"a": sample.split("\n")})

with open("./inputs/day5.txt") as ff:
    intxt = ff.read()
df = pl.DataFrame({"a": intxt.split("\n")})


seeds, mapping = df[0:1], df[1:]
seeds = (
    seeds.select(seed=pl.col("a").str.split(": ").list.get(1).str.split(" "))
    .explode("seed")
    .select(pl.col("seed").cast(pl.Int64))
)

mapping = (
    mapping.with_columns(
        colstruct=pl.when(
            pl.col("a").str.slice(0, 1).cast(pl.Int64, strict=False).is_not_null()
        )
        .then(pl.struct(heading=pl.lit(None, pl.Utf8), nums=pl.col("a")))
        .otherwise(pl.struct(heading=pl.col("a"), nums=pl.lit(None, pl.Utf8)))
    )
    .unnest("colstruct")
    .drop("a")
    .with_columns(pl.col("heading").forward_fill())
    .filter(pl.col("nums").is_not_null())
    .with_columns(
        pl.col("nums").str.split(" ").list.to_struct(fields=["dest", "src", "n"])
    )
    .unnest("nums")
    .with_columns(
        pl.col("heading")
        .str.replace(" map:", "")
        .str.split("-to-")
        .list.to_struct(fields=["src_type", "dest_type"]),
        pl.col("dest", "src", "n").cast(pl.Int64),
    )
    .unnest("heading")
    .select("src_type", "dest_type", "src", "dest", "n")
    .sort("src")
)


def next_map(df):
    this_col = df.columns[-1]
    next_col = mapping.filter(pl.col("src_type") == this_col)["dest_type"][0]
    return (
        df.sort(this_col)
        .join_asof(
            mapping.filter(pl.col("src_type") == this_col).drop(
                "src_type", "dest_type"
            ),
            left_on=this_col,
            right_on="src",
        )
        .with_columns(
            (
                pl.when(pl.col("src") + pl.col("n") > pl.col(this_col))
                .then(pl.col(this_col) - pl.col("src") + pl.col("dest"))
                .otherwise(pl.col(this_col))
            ).alias(next_col)
        )
        .drop("src", "dest", "n")
    )


master_guide = (
    seeds.pipe(next_map)
    .pipe(next_map)
    .pipe(next_map)
    .pipe(next_map)
    .pipe(next_map)
    .pipe(next_map)
    .pipe(next_map)
).sort("location")

## Part Two
seeds2 = (
    df[0:1]
    .select(seed=pl.col("a").str.split(": ").list.get(1).str.split(" "))
    .explode("seed")
    .with_columns(coln=pl.first().cum_count().mod(2), i=(pl.first().cum_count() // 2))
    .pivot("seed", "i", "coln", "first")
    .rename({"0": "seed", "1": "range"})
    .drop("i")
    .with_columns(pl.all().cast(pl.Int64))
    .select("range", "seed")
)
### I can not figure out how to get the joins to work to do this with ranges at the start.
## Brute force, guess and check seeds


def all_map(df, _print=False, _return_df=False):
    """Function to return all of the mappings given an input seed df"""
    df = (
        df.pipe(next_map)
        .pipe(next_map)
        .pipe(next_map)
        .pipe(next_map)
        .pipe(next_map)
        .pipe(next_map)
        .pipe(next_map)
    ).sort("location")
    if _print:
        print(f"best location is {df['location'][0]} from seed {df['seed'][0]}")
        return None
    else:
        if _return_df:
            return df
        else:
            return df["location"][0]


def min_func(x):
    """wrapper for all_map to only take a single input which is expected to be between 0 and 1
    and is multiplied by range and added to the existings seed"""
    return seeds2.with_columns(
        seed=(pl.col("seed") + pl.lit(x) * pl.col("range")).cast(pl.Int64)
    ).pipe(all_map)


# Run min_func with 0.001 increments for the input ranges
res = {}
for x in range(1000):
    res[str(x / 1000)] = min_func(x / 1000)

res_df = pl.from_dict(res).melt().sort("value")

multiplier = float(res_df[0, 0])

starting_seed = all_map(
    seeds2.with_columns(
        seed=(pl.col("seed") + pl.lit(multiplier) * pl.col("range")).cast(pl.Int64)
    ),
    _return_df=True,
)["seed"][0]


# The previous result should be (knock on wood) close but with
# such big numbers it could easily be smaller still. Now instead of multiplying, just subtract
# (arbitrarily chosen) 10000 at a time until the location gets bigger than previous best location.
x = 1
while True:
    ret = all_map(
        pl.select(
            seed=pl.int_range(
                pl.lit(starting_seed) - pl.lit(x) * pl.lit(10000),
                pl.lit(starting_seed) - pl.lit(x - 1) * pl.lit(10000),
            )
        ),
        _return_df=True,
    )
    if ret["location"][0] > res_df["value"][0]:
        x = x - 1
        ret = all_map(
            pl.select(
                seed=pl.int_range(
                    pl.lit(starting_seed) - pl.lit(x) * pl.lit(10000),
                    pl.lit(starting_seed) - pl.lit(x - 1) * pl.lit(10000),
                )
            ),
            _return_df=True,
        )
        break
    else:
        x = x + 1


# location = 79_874_951
# humidity = 493_450_090
# temperature = 1_244_941_821
# light = 2_027_995_352
# water = 1_865_108_284
# fertilizer = 934_422_079
# soil = 3_969_171_811
# seed = 3_969_171_811


## I only know this worked b/c the site said it was the right answer,
# otherwise I could have been in a local min without knowing it. Next stop if this didn't work
# would have been to rerun the min_func loop with smaller granularity.
