import pickle
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import os


def generate_key(password, salt):
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    return base64.urlsafe_b64encode(kdf.derive(password))

module_path = os.path.abspath(os.path.dirname(__file__))
enc_file_path = os.path.join(module_path, 'financial_constants.enc')

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

korea_financial_column = loaded_data["korea_financial_column"]
fmp_FMP_field = loaded_data["fmp_FMP_field"]
fmp_core_cols = loaded_data["fmp_core_cols"]
fmp_is_cols = loaded_data["fmp_is_cols"]
fmp_bs_cols = loaded_data["fmp_bs_cols"]
fmp_cf_cols = loaded_data["fmp_cf_cols"]

