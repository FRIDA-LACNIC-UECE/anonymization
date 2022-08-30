import anonypy
import pandas as pd
import sqlite3
import time

def t_closeness(table, connection, df, columns, sensitive_column) :
    #columns with text elements
    categorical = set(("field2", "field4", "field6", "field7", "field8", "field9", "field10", "field14", "field15")) 

    for name in categorical:
        df[name] = df[name].astype("category")

    p = anonypy.Preserver(df, columns, sensitive_column)
    rows = p.anonymize_t_closeness(k = 5, p = 0.2)
    
    new_df = pd.DataFrame(rows)
    new_table = "anony_" + table
    new_df.to_sql(new_table, connection, if_exists = "replace", index = False)


if __name__ == "__main__":  

    start = time.time()

    db = "test.db"
    table = "adult"

    connection = sqlite3.connect(db)
    cursor = connection.cursor()

    columns = []
    data = cursor.execute("SELECT * from " + table + " limit 1")
    for column in data.description :
        columns.append(column[0])

    query = "SELECT * from " + table 
    sql_query = pd.read_sql_query(query, connection)
    df = pd.DataFrame(data = sql_query, columns = columns)

    sensitive_column = "field9"
    t_closeness(table, connection, df, columns, sensitive_column)

    total = time.time() - start
    print(total)

    connection.commit()
    cursor.close()
