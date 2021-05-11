1. Make sure all the requirements.txt are installed --> pip install -r requirements.txt
2. Run the python program with python3.6 --> python covid_data.py

Assumptions:

1. Currently we are doing a full refresh of all the tables into stage and load the delta to final table considering no outage on final table
2. In order to eliminate outage we only insert the data which is missing in final table

Future Enhancements:

1. Use the latest API for extracting only required data "https://dev.socrata.com/foundry/health.data.ny.gov/xdss-u53e"
2. Add multithreading to extract multiple counties at the same time
3. Adding configparser according to the need of development and production environment