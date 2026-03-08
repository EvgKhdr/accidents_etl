import requests
import os
from dotenv import load_dotenv
import json

def load_data():
    range_years = range(2005, 2020) #api is able to retreive info from year 2005 to 2019 
    years= list(range_years)
    directory_name = "json_accidents_data"
    load_dotenv()
    api_key = os.getenv("API_KEY")
    try:
        os.mkdir(directory_name)
        print(f"Directory '{directory_name}' created successfully.")
    except FileExistsError:
        print(f"Directory '{directory_name}' already exists.")
    except PermissionError:
        print(f"Permission denied: Unable to create '{directory_name}'.")
    except Exception as e:
        print(f"An error occurred: {e}")

    for year in years:
        try:
            response = requests.get(
                f"https://api.tfl.gov.uk/AccidentStats/{year}",
                params={"app_key": api_key},
                timeout=30
            )
            response.raise_for_status()

            data = response.json()

            with open(os.path.join(directory_name, f"tfl_accidents_{year}.json"), "w") as f:
                json.dump(data, f, indent=4)
            print(f"Saved! tfl_accidents_{year}.json")

        except requests.exceptions.Timeout:
            print(f"Year {year}: timed out — skipping")
        except requests.exceptions.HTTPError as e:
            print(f"Year {year}: HTTP error {e.response.status_code} — skipping")
        except requests.exceptions.ConnectionError:
            print(f"Year {year}: no internet connection — skipping")
        except requests.exceptions.RequestException as e:
            print(f"Year {year}: request failed — {e}")
        except Exception as e:
            print(f"Year {year}: unexpected error — {e}")


load_data()
