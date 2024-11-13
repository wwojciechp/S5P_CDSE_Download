 # Import necessary libraries
import json
import requests
import os
from getpass import getpass
from tqdm import tqdm

# Function to obtain Keycloak access token
def get_keycloak(username: str, password: str) -> str:
    # Prepare authentication data
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password"
    }

    try:
        # Send POST request to Keycloak server
        r = requests.post("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
                          data=data)
        r.raise_for_status()
    except Exception as e:
        # Handle authentication failure
        raise Exception(
            f"Keycloak token creation failed. Response from the server was: {r.json()}"
        )

    return r.json()["access_token"], r.json()["refresh_token"]

# Function to refresh Keycloak access token
def refresh_keycloak(refresh_token: str) -> str:
    # Prepare data for token refresh
    data = {
        "client_id": "cdse-public",
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    try:
        # Send POST request to Keycloak server for token refresh
        r = requests.post("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
                          data=data)
        r.raise_for_status()
    except Exception as e:
        # Handle token refresh failure
        raise Exception(
            f"Keycloak token creation failed. Response from the server was: {r.json()}"
        )

    return r.json()["access_token"], r.json()["refresh_token"]

# Get password from user securely
passwd = str(getpass())

# Obtain initial Keycloak tokens
keycloak_token, refresh_token = get_keycloak("email@example.com", passwd) #change e-mail

# Function to download file with progress bar
def download(url: str, fname: str, chunk_size=1024):
    session = requests.Session()
    headers = session.headers.update({'Authorization': 'Bearer {}'.format(keycloak_token)})

    resp = requests.get(url, allow_redirects=False)

    # Handle redirects
    while resp.status_code in (301, 302, 303, 307):
        url = resp.headers['Location']
        resp = session.get(url, verify=True, stream=True, allow_redirects=False)
        total = int(resp.headers.get('content-length', 0))

        # Download file in chunks with progress bar
        with open(fname, 'wb') as file, tqdm(
            desc=fname,
            total=total,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for data in resp.iter_content(chunk_size=chunk_size):
                size = file.write(data)
                bar.update(size)

print("Searching for products: ")

# URL for querying products
products_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?&$filter=((Collection/Name%20eq%20%27SENTINEL-5P%27%20and%20(Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27instrumentShortName%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27TROPOMI%27)%20and%20(contains(Name,%27L2__NO2___%27)%20and%20OData.CSC.Intersects(area=geography%27SRID=4326;POLYGON%20((-8.09%2024.61,%2049.56%2024.61,%2049.56%2066.88,%20-8.09%2066.88,%20-8.09%2024.61))%27)))%20and%20Online%20eq%20true)%20and%20ContentDate/Start%20ge%202023-08-01T00:00:00.000Z%20and%20ContentDate/Start%20lt%202023-12-31T23:59:59.999Z)&$orderby=ContentDate/Start%20desc&$expand=Attributes&$count=True&$top=50&$expand=Assets&$skip=0" #paste OData Querry here

session = requests.Session()
headers = session.headers.update({'Authorization': 'Bearer {}'.format(keycloak_token)})
response = requests.get(products_url, headers=headers)

# Parse JSON response
lines = json.loads(response.text)

destination_folder = "./data" #change destination

# Iterate through products and download them
for value_data in lines["value"]:
    print('\033[0;37mDownloading product \033[1;31m' + value_data["Name"] + '\033[0;37m with id: \033[1;36m' + value_data["Id"] + '\033[0;37m')

    zip_file_name = value_data["Name"] + ".zip"
    product_identyficator = str(value_data['Id'])
    zip_file_path = os.path.join(destination_folder, zip_file_name)

    # Refresh Keycloak token before each download
    url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_identyficator})/$value"
    keycloak_token, refresh_token = refresh_keycloak(refresh_token)

    download(url, zip_file_path)
