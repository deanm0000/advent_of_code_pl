import polars as pl

# sample="""1abc2
# pqr3stu8vwx
# a1b2c3d4e5f
# treb7uchet"""

# df=pl.DataFrame({'a':sample.split('\n')})

with open("day1_input.txt") as ff:
    intxt = ff.read()

df = pl.DataFrame({"a": intxt.split("\n")})

part_one_ans = (
    df.with_columns(b=pl.col("a").str.extract_all("\d")).with_columns(
        c=pl.col("b").list.first().cast(pl.Int32) * 10
        + pl.col("b").list.last().cast(pl.Int32)
    )
)["c"].sum()


##Part Two
# sample="""two1nine
# eightwothree
# abcone2threexyz
# xtwone3four
# 4nineeightseven2
# zoneight234
# 7pqrstsixteen"""
# df=pl.DataFrame({'a':sample.split('\n')})

nums = "one, two, three, four, five, six, seven, eight, nine"
nums = nums.split(", ")
nums = {num: x + 1 for x, num in enumerate(nums)}


def repl_nums(col):
    for pattern, repl in nums.items():
        col = col.str.replace_all(pattern, pattern[0] + str(repl) + pattern[-1])
    return col


df = df.with_columns(repl_nums(pl.col("a")))

part_two_ans = (
    df.with_columns(b=pl.col("a").str.extract_all("\d")).with_columns(
        c=pl.col("b").list.first().cast(pl.Int32) * 10
        + pl.col("b").list.last().cast(pl.Int32)
    )
)["c"].sum()
