import pandas as pd
import os
from cryptography.fernet import Fernet
from dotenv import load_dotenv

load_dotenv()
KEY = os.getenv('KEY').encode()
fernet = Fernet(KEY)

def encrypt_value(value):
    if pd.isna(value):
        return value
    value_str = str(value)
    encrypted = fernet.encrypt(value_str.encode())
    return encrypted.decode()

def decrypt_value(value):
    if pd.isna(value):
        return value
    decrypted = fernet.decrypt(value)
    return decrypted.decode()

def encrypt_table(df):
    df_encrypted = df.map(encrypt_value)
    return df_encrypted

def decrypt_table(df):
    df_decrypted = df.map(decrypt_value)
    return df_decrypted