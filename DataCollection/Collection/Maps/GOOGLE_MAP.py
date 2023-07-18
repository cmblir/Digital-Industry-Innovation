import urllib, os

class information:
    def __init__(self):
        self.print_information()

    def print_information(self):
        print("""
        함수에 대한 설명은 아래와 같습니다. \n
        라이브러리 내 주요 클래스는 map입니다. \n
        GetStreet()는 구글 map api를 호출하는 함수입니다.. \n
        collect()은 데이터를 추출하는 함수입니다.
        """)

class map:
    def __init__(self):
        self.key = "AIzaSyA_aFyISdRheOULp2nv8q4qu-UtsdY_CBU"
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