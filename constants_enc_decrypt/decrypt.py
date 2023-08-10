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

password = input("input your password").encode()

# 파일에서 암호화된 데이터 불러오기
with open('data.enc', 'rb') as file:
    salt = file.read(16)  # Salt 읽기
    encrypted_data = file.read()

key = generate_key(password, salt)

# 암호화 객체 생성
cipher_suite = Fernet(key)

# 복호화
decrypted_data = cipher_suite.decrypt(encrypted_data)

# Pickle에서 데이터 로드
loaded_data = pickle.loads(decrypted_data)

print(loaded_data)