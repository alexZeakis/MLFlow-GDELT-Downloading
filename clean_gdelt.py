import requests

# CKAN API endpoint and API key
api_url = 'http://10.230.186.79:9058/api/3/action/'
headers = { "Authorization" : "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJjSWU2Mk10SEJUQk1TTFRLaDI0cHRwdjkwVHAzdXNUbkMzSk5kcjJYNEs4IiwiaWF0IjoxNjc0NDgxNDAwfQ.OM58qcAtvLrihMvuhBRhEWWYvMO-59C3ZnXmsZDCX5w" ,
           }

# Function to delete datasets by tag
def delete_datasets_by_tag(tag):
    # Get a list of datasets with the given tag
    search_url = api_url + 'package_search'
    search_params = {
        'q': f'tags:{tag}',
        'rows': 1000,  # Maximum number of datasets per request,
        'include_private': True
    }
    response = requests.post(search_url, json=search_params, headers=headers)
    response_json = response.json()

    if response.status_code == 200:
        # Extract the dataset IDs from the response
        dataset_ids = [result['id'] for result in response_json['result']['results']]
        
        if dataset_ids:
            # Delete each dataset by ID
            for dataset_id in dataset_ids:
                delete_url = api_url + 'package_delete'
                delete_params = {
                    'id': dataset_id,
                }
                delete_response = requests.post(delete_url, json=delete_params, headers=headers)
                if delete_response.status_code == 200:
                    print(f"Deleted dataset with ID: {dataset_id}")
                else:
                    print(f"Failed to delete dataset with ID: {dataset_id}")
        else:
            print("No datasets found with the specified tag.")
    else:
        print("Failed to retrieve dataset list.")

# Call the function to delete datasets with the tag "STELAR_GDELT"
delete_datasets_by_tag('STELAR_GDELT')

