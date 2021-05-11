import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import etl


def response_json(url):
    """
    The Function takes a url and the response is returned
    as json data
    Args:
    url: Provide the url for capturing the json data.
    Returns:
    data
    """
    response = requests.get(url, verify=False)
    if response.status_code == 200:
        read_data = response.json()
        return read_data
    else:
        print(
            f"Please check the URL and make sure its working fine, Currently the request failed with status code: {response.status_code}"
        )
        raise Exception


def columns_view(read_data):
    """
    The Function reads data from our response
    and maps to the columns we are looking for
    Args:
    read_date: response data from the json
    Returns:
    column_names
    """
    list_column = read_data["meta"]["view"]["columns"]
    column_names = [column_dict["name"] for column_dict in list_column]
    return column_names


def table_dataframe(read_data, column_names):
    """
    The Function reads data from our response
    and creates a pandas frame
    Args:
    read_date: response data from the json
    column_names: provide the columns names for your dataset
    Returns:
    pandas dataframe with specific columns
    """
    new_york_data_full = pd.DataFrame(data=read_data["data"], columns=column_names)
    new_york_data_full["Load_date"] = pd.Timestamp("today").strftime("%m/%d/%Y")
    ny_data = new_york_data_full[
        [
            "Test Date",
            "County",
            "New Positives",
            "Cumulative Number of Positives",
            "Total Number of Tests Performed",
            "Cumulative Number of Tests Performed",
            "Load_date",
        ]
    ]
    ny_data.columns = ny_data.columns.str.replace(" ", "_")
    return ny_data


def create_county_frame(new_york_data):
    """
    The Function reads data from pandas dataframe
    and does a group by and lists counties and data for
    the same
    Args:
    new_york_data: response data from the json
    Returns:
    dictonary object with county as key and data as values
    """
    result = [x.reset_index(drop=True) for _, x in new_york_data.groupby(["County"])]
    county_dict = {}
    for county in result:
        county["County"][0] = county["County"][0].replace(" ", "_")
        county["County"][0] = county["County"][0].replace(".", "")
        county_dict[county["County"][0]] = county
    return county_dict


def main():
    url = "https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD"

    read_data = response_json(url)
    column_names = columns_view(read_data)
    ny_data = table_dataframe(read_data, column_names)
    county_dict = create_county_frame(ny_data)

    print(f"There are {len(list(county_dict.keys()))} counties from the dataframe")

    conn = etl.create_database("ny_covid.sqlite")
    func = partial(etl.create_tables_county_m, ny_data, conn)
    with ThreadPoolExecutor(max_workers=4) as t:
        t.map(func, list(county_dict.keys()))

    etl.create_final_table(conn, list(county_dict.keys()))

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
