
def sql_create_table(
    table_name,
    columns,
    primary_key,
    if_not_exists=True,
    noJsonB = False
):
    # Quote table name
    safe_table = f'"{table_name}"'

    # Quote reserved keywords
    def quote_col(col):
        reserved = {"to", "from", "order", "group", "user", "select", "where"}
        return f'"{col}"' if col.lower() in reserved else col

    # Build column definitions
    col_defs = []
    for col_name, col_type in columns.items():
        if noJsonB:
            col_type = col_type.replace('JSONB', 'JSON')
        col_defs.append(f"    {quote_col(col_name)} "+ f"{col_type}")

    # # Add JSONB column
    # if jsonb_column:
    #     col_defs.append(f"    {quote_col(jsonb_column)} JSONB DEFAULT '{{}}'")

    # Primary key
    if primary_key:
        pk_cols = ", ".join(quote_col(c) for c in primary_key)
        col_defs.append(f"    PRIMARY KEY ({pk_cols})")

    # Join
    columns_sql = ",\n".join(col_defs)
    exists_clause = "IF NOT EXISTS " if if_not_exists else ""
    return f"""
CREATE TABLE {exists_clause}{safe_table} (
{columns_sql}
);
""".strip()




def sql_create_hypertable(name, indexer,  chunkSize = 10000):
    return f"""
SELECT create_hypertable(
    '"{name}"',
    '{indexer}',
    chunk_time_interval => {chunkSize},
    if_not_exists => TRUE,
    migrate_data => TRUE
);
"""
def build_upsert_sql(cols, name, conflict=[]):
    # Quote reserved keywords
    quoted_cols = []
    for c in cols:
        if c in ("to", "from"):  # reserved keywords
            quoted_cols.append(f'"{c}"')
        else:
            quoted_cols.append(c)

    if conflict:
        set_clause = ", ".join(
            f'"{c}" = EXCLUDED."{c}"'
            for c in cols
            if c not in conflict
        )
        
        conflict_cols = ', '.join(conflict)
    return f"""
    INSERT INTO "{name}" ({', '.join(quoted_cols)})
    VALUES({', '.join(['?']*len(cols))})
    {f'ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause}' if conflict else ''}
    
    """
def sql_append(table, data):
    f'''insert into {table} '''
def sql_create_index(name, uniqueKeys, needsUnique):
    unique_cols = ", ".join(f'{col}' for col in uniqueKeys)
    index_name = f"idx_{name}_unique"
    return f"""
    CREATE {'UNIQUE' if needsUnique else ''} INDEX IF NOT EXISTS {index_name}
    ON "{name}" ({unique_cols});
    """.strip()
