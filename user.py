import jwt
import duckdb
import hashlib
import dotenv
import os
import declare_constants
from pydantic import BaseModel
dotenv.load_dotenv()

class User(BaseModel):
    username: str

def hash_password(password: str):
    return hashlib.sha256(password.encode()).hexdigest()

