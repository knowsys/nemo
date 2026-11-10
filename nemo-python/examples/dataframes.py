"""A dataframe in, reasoning, and a dataframe out, through Arrow.

Usage: `python dataframes.py`, with the `nmo` module installed (`maturin develop`),
and with pyarrow and pandas.
"""

import sys

import nmo

try:
    import pandas
    import pyarrow
except ImportError as missing:
    sys.exit(f"this example needs pyarrow and pandas: {missing}")

readings = pandas.DataFrame(
    {
        "sensor": ["north", "north", "south", "south", "south"],
        "hour": [1, 2, 1, 2, 3],
        "degrees": [18.5, 21.0, 24.0, 26.5, 31.0],
    }
)

# Rows go in as an iterable of tuples. `itertuples` yields Python values,
# so a text column arrives as strings, and a number column as integers or doubles.
engine = nmo.Engine()
engine.import_rows("reading", readings.itertuples(index=False))
engine.load(
    nmo.Program.parse(
        """
        warmest(?sensor, #max(?degrees)) :- reading(?sensor, ?hour, ?degrees) .
        hot(?sensor, ?hour) :- reading(?sensor, ?hour, ?degrees), ?degrees > 25.0e0 .
        """
    )
)
engine.reason()

# Results come out as an Arrow stream. A column whose values are all integers, all doubles
# or all booleans has that type, and any other column is text.
# The columns have no names of their own, so the frame gets them here.
warmest = engine.to_pandas("warmest")
warmest.columns = ["sensor", "degrees"]
print(warmest.sort_values("sensor").to_string(index=False))

# `arrow_stream` needs no pyarrow on the engine's side: any library that reads the Arrow
# C stream interface takes it, such as pyarrow, polars or duckdb.
stream = engine.arrow_stream("hot")
table = pyarrow.table(stream)
print(len(stream), "rows as", table.schema.types)
