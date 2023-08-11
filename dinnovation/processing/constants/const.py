import pickle
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import os
import sqlalchemy

def generate_key(password, salt):
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    return base64.urlsafe_b64encode(kdf.derive(password))

module_path = os.path.abspath(os.path.dirname(__file__))
enc_file_path = os.path.join(module_path, 'processing_constants.enc')

with open(enc_file_path, 'rb') as file:
    salt = file.read(16)
    encrypted_data = file.read()

password = input("input your password").encode()

key = generate_key(password, salt)

# create encryption object
cipher_suite = Fernet(key)

# decryption
decrypted_data = cipher_suite.decrypt(encrypted_data)

# Load data from Pickle
loaded_data = pickle.loads(decrypted_data)

country = loaded_data["country"]
dtypesql_finan = loaded_data["dtypesql_finan"]
definition_finan = loaded_data["definition_finan"]
dtypesql_info = loaded_data["dtypesql_info"]
definition_info = loaded_data["definition_info"]
stock_name = loaded_data["stock_name"]
TableDefaultColumns = loaded_data["TableDefaultColumns"]
TableDefaultColumns_info = loaded_data["TableDefaultColumns_info"]
TableDefault = loaded_data["TableDefault"]
definition = loaded_data["definition"]
ColumnsDict = loaded_data["ColumnsDict"]
Financial = loaded_data["Financial"]
Month = loaded_data["Month"]
NumericList = loaded_data["NumericList"]
MonthDict = loaded_data["MonthDict"]
stock_info = loaded_data["stock_info"]