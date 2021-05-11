import sqlite3
from sqlite3 import Error


def create_database(databasefile):
    """
    The Function creates a database connection
    Returns:
    connection
    """
    conn = None
    try:
        conn = sqlite3.connect(databasefile, check_same_thread=False)
    except Error as e:
        print(e)

    return conn


def create_tables_county_m(new_york_data, conn, county):
    """
    This function is used to create SQLite tables from pandas dataframe
    Args:
    new_york_data: response data from the json
    conn: Database connection
    county: list of counties
    """

    try:
        # Write the new DataFrame to a new SQLite table
        new_york_data[(new_york_data.County == county)].to_sql(
            county + "_stg", conn, if_exists="replace"
        )
    except Exception as e:
        print(f"Failed while creating stage table for {county} with exception {e}")
    finally:
        conn.commit()


def load_final_table(conn, county):
    """
    This function is used to load the final SQLite tables from stage tables
    Args:
    conn: Database connection
    county: list of counties
    """
    query_insert = (
        "INSERT INTO "
        + county
        + " SELECT * FROM "
        + county
        + "_stg WHERE TEST_DATE > (SELECT MAX(TEST_DATE) FROM "
        + county
        + ");"
    )

    try:
        execute_query(conn, query_insert)
    except Exception as e:
        print(f"{query_insert} has failed with below error {e}")


def create_final_table(conn, county):
    """
    This function is used to create SQLite final tables for the first time
    or in the case of new county data is provided
    Args:
    conn: Database connection
    county: list of counties
    """
    for county in county:
        query = f"SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%' AND name = '{county}'"
        result = execute_query(conn, query)
        try:
            if len(result) == 0:
                query = f"create table {county} as select * from {county}_stg;"
                execute_query(conn, query)

            load_final_table(conn, county)
        except Exception as e:
            print(f"This query {query} failed with exception {e}")


def execute_query(conn, query):
    """
    The Function is used to run queries on SQLite database
    Args:
    conn: provide the current connection
    query: provide the query to be executed
    """
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    return cur.fetchall()
