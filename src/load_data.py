import sqlite3

def store_results(df, db_name, table_name):
    """
    Stores the given DataFrame into a SQLite database table.
    Parameters:
    df (pandas.DataFrame): The DataFrame to be stored.
    db_name (str): The name of the SQLite database file.
    table_name (str): The name of the table where the DataFrame will be stored.
    Notes:
    If the table already exists, it will be replaced.
    """
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

    #add replace in the documentation