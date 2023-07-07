def gen_csr_creation_query(csr_id, src_ref_table, edge_table, src_ref_col = None, src_key = None, dst_key = None, dst_ref_table = None, dst_ref_col = None):
    csr_creation_query = """SELECT  CREATE_CSR_EDGE(
            {csr_id},
            (SELECT count(srctable.{src_ref_col}) FROM {src_ref_table} srctable),
            CAST (
                (SELECT sum(CREATE_CSR_VERTEX(
                            {csr_id},
                            (SELECT count(srctable.{src_ref_col}) FROM {src_ref_table} srctable),
                            sub.dense_id,
                            sub.cnt)
                            )
                FROM (
                    SELECT srctable.rowid as dense_id, count(edgetable.{src_key}) as cnt
                    FROM {src_ref_table} srctable
                    LEFT JOIN {edge_table} edgetable ON edgetable.{src_key} = srctable.{src_ref_col}
                    GROUP BY srctable.rowid) sub
                )
            AS BIGINT),
            srctable.rowid,
            dsttable.rowid,
            edgetable.rowid) as temp
        FROM {edge_table} edgetable
        JOIN {src_ref_table} srctable on srctable.{src_ref_col} = edgetable.{src_key}
        JOIN {dst_ref_table} dsttable on dsttable.{dst_ref_col} = edgetable.{dst_key} ;
    """
    if src_ref_col == None:
        src_ref_col = "id"
    if src_key == None:
        src_key = "src"
    if dst_key == None:
        dst_key = "dst"
    if dst_ref_table == None:
        dst_ref_table = src_ref_table
    if dst_ref_col == None:
        dst_ref_col = src_ref_col
    return csr_creation_query.format(csr_id = csr_id, src_ref_table = src_ref_table, src_ref_col = src_ref_col, edge_table = edge_table, src_key = src_key, dst_key = dst_key, dst_ref_table = dst_ref_table, dst_ref_col = dst_ref_col)


def gen_select_query1(table_name):
    return f"select dst from {table_name} where dst + src > 1000"
