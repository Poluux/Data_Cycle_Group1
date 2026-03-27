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

def encrypt_table(df, encrypted_columns=None):
    df_encrypted = df.copy()
    cols_to_encrypt = [c for c in encrypted_columns if c in df.columns] if encrypted_columns else df.columns
    for col in cols_to_encrypt:
        df_encrypted[col] = df_encrypted[col].map(encrypt_value)
    return df_encrypted

def decrypt_table(df, encrypted_columns=None):
    df_decrypted = df.copy()
    cols_to_decrypt = [c for c in encrypted_columns if c in df.columns] if encrypted_columns else df.columns
    for col in cols_to_decrypt:
        df_decrypted[col] = df_decrypted[col].map(decrypt_value)
    return df_decrypted