import duckpgq
import time
import dgl # reduce the time for first dgl export
import data_loaders as dtldr
import genquery
import torch
from torch_geometric.data import Data, HeteroData
from torch_geometric.typing import SparseTensor
SIZES = [1, 3, 10, 30, 100]
TASKS = ["getpg", "csr_creation", "select"]
TYPES = ["table", "pd.DataFrame", "pl.DataFrame", "pyarrow.Table"]

DATA_LOADERS = {"pd.DataFrame": dtldr.pddfloader, "pl.DataFrame": dtldr.pldfloader, "pyarrow.Table": dtldr.patableloader}

def dbinit():
    z1 = """CREATE TABLE Student(id BIGINT, name VARCHAR);
    CREATE TABLE know(src BIGINT, dst BIGINT, id BIGINT);
    CREATE TABLE School(school_name VARCHAR, school_id BIGINT, school_kind BIGINT);
    INSERT INTO Student VALUES (0, 'Daniel'), (1, 'Tavneet'), (2, 'Gabor'), (3, 'Peter'), (4, 'David');
    INSERT INTO know VALUES (0,1, 10), (0,2, 11), (0,3, 12), (3,0, 13), (1,2, 14), (1,3, 15), (2,3, 16), (4,3, 17), (2, 4, 18);"""
    z2 = """-CREATE PROPERTY GRAPH pggggg
    VERTEX TABLES (Student PROPERTIES ( id, name ) LABEL Person)
    EDGE TABLES (know    SOURCE KEY ( src ) REFERENCES Student ( id )
            DESTINATION KEY ( dst ) REFERENCES Student ( id )
            PROPERTIES ( id ) LABEL Knows);"""
    #con = duckpgq.connect()
    con = duckpgq.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
    con.sql("force install '/duckdb/build/release/extension/duckpgq/duckpgq.duckdb_extension';")
    con.sql("load 'duckpgq';")
    con.sql(z1)
    con.sql(z2)
    return con

def prepare_data(t, size):
    if t not in TYPES:
        raise Exception(f"Type {t} is not supported")
    if size not in SIZES:
        raise Exception(f"Size {size} is not supported")
    efile = "snb-bi-sf{}.e".format(size)
    vfile = "snb-bi-sf{}.v".format(size)
    con = dbinit()
    #con = duckpgq.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
    #con.sql("force install '/duckdb/build/release/extension/duckpgq/duckpgq.duckdb_extension';")
    #con.sql("load 'duckpgq';")
    if t == "table":
        edgedf, nodedf = DATA_LOADERS["pd.DataFrame"](efile, vfile, False)
        con.sql("create table edges as select * from edgedf")
        con.sql("create table nodes as select * from nodedf")
        edgedf = "edges" 
        nodedf = "nodes"
    elif t in DATA_LOADERS:
        edgedf, nodedf = DATA_LOADERS[t](efile, vfile)
    else:
        raise Exception(f"BUG: Type {t} is not a table, and it does not have a loader.")
    return con, edgedf, nodedf

def gen_query(t, task):
    if t not in TYPES:
        raise Exception(f"Type {t} is not supported")
    if task not in TASKS:
        raise Exception(f"Task {task} is not supported")
    edge_table = "edgedf"
    node_table = "nodedf"
    if t == "table":
        edge_table = "edges"
        node_table = "nodes"
    if task == "select":
        return genquery.gen_select_query1(edge_table)
    elif task == "csr_creation":
        return genquery.gen_csr_creation_query(0, node_table, edge_table)
    elif task == "getpg":
        edge_table = "edges"
        node_table = "nodes"
        query = f"""-CREATE PROPERTY GRAPH mypg VERTEX TABLES ( {node_table} PROPERTIES ( id ) LABEL Person)
    EDGE TABLES ({edge_table}    SOURCE KEY ( src ) REFERENCES {node_table} ( id )
            DESTINATION KEY ( dst ) REFERENCES {node_table} ( id )
            PROPERTIES ( weight ) LABEL Knows);"""
        return (query, genquery.gen_csr_creation_query(0, node_table, edge_table))

def pipeline(t, size, task):
    con, edgedf, nodedf = prepare_data(t, size)
    #query = gen_query(t, task)
    if task == "getpg":
        query = gen_query(t, task)
        con.sql(query[0])
        con.sql(query[1]).fetchall()
        start = time.time()
        z = con.get_dgl("mypg", 0)
        end = time.time()
    elif task == "csr_creation":
        start = time.time()
        con.create_csr(edgedf, "src", "dst", nodedf, "id")
        end = time.time()
    else:
        query = gen_query(t, task)
        start = time.time()
        con.sql(query).fetchnumpy()
        end = time.time()
    return (end - start) * 1000

def run():
    results = []
    for task in TASKS:
        for size in SIZES:
            for t in TYPES:
                if task == "getpg" and t != "table":
                    continue
                exectime = pipeline(t, size, task)
                results.append((task, size, t, exectime))
                #print(f"Task {task} with size {size} on {t} takes {exectime} ms", flush = True)
    return results

if __name__ == "__main__":
    res = run()
    print(res)
