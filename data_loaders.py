def pddfloader(efile, vfile, with_row_id = True):
    import pandas as pd
    edgedf = pd.read_csv(efile, header = None, names = ["src", "dst", "weight"], delimiter = ' ')
    nodedf = pd.read_csv(vfile, header = None, names = ["id"], delimiter = ' ')
    if with_row_id:
        nodedf["rowid"] = nodedf.index
        edgedf["rowid"] = edgedf.index
    return edgedf, nodedf

def patableloader(efile, vfile, with_row_id = True):
    from pyarrow import csv
    import pyarrow as pa
    import numpy as np
    parse_options = csv.ParseOptions(delimiter=" ")
    read_options = csv.ReadOptions(column_names=["src", "dst", "weight"])
    edgedf = csv.read_csv(efile, read_options, parse_options)
    read_options = csv.ReadOptions(column_names=["id"])
    nodedf = csv.read_csv(vfile, read_options, parse_options)
    if with_row_id:
        edgedf = edgedf.append_column("rowid", pa.array(np.arange(edgedf.num_rows)))
        nodedf = nodedf.append_column("rowid", pa.array(np.arange(nodedf.num_rows)))
    return edgedf, nodedf

def pldfloader(efile, vfile, with_row_id = True):
    import polars as pl
    edgedf = pl.read_csv(efile, has_header = False, new_columns = ["src", "dst", "weight"], separator = ' ')
    nodedf = pl.read_csv(vfile, has_header = False, new_columns = ["id"], separator = ' ')
    if with_row_id:
        edgedf = edgedf.with_row_count("rowid")
        nodedf = nodedf.with_row_count("rowid")
    return edgedf, nodedf
