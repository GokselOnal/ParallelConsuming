from secret import SecretFile
import requests

class APIdata:
    def __init__(self):
        self.api_url = 'https://api.api-ninjas.com/v1/historicalevents?year=1945&offset='

    def get_data(self):
        result = []
        offset = "0"
        for i in range(10):
            response = requests.get(self.api_url + offset, headers={'X-Api-Key': SecretFile.api_key})
            if response.status_code == requests.codes.ok:
                for j in response.json():
                    result.append(j)
            offset = str(int(offset) + 10)
        return result