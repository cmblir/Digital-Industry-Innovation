import urllib, os

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        A description of the function is given below. \n
        The main class in the library is map. \n
        GetStreet() is a function that calls the Google map api. \n
        collect() is a function that extracts data.
        """)

class map:
    def __init__(self, api_key):
        self.key = api_key
        self.base = "https://maps.googleapis.com/maps/api/streetview"
        
    def GetStreet(self, address, save_loc):
        params = {"size":"1920x1080", "location":address, "key":self.key}
        url = self.base + "?" + urllib.parse.urlencode(params)
        if "\n" in address: address = address.replace("\n", "")
        filename = address.replace(",", "").replace(" ", "_") + ".bmp"
        urllib.request.urlretrieve(url, os.path.join(save_loc, filename))

    def collect(self, address_lst, save_loc):
        for address in address_lst:
            self.GetStreet(address, save_loc)