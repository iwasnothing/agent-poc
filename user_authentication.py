import jwt
import duckdb
import hashlib
from user import hash_password,User
import dotenv
import os
import declare_constants
from pydantic import BaseModel
dotenv.load_dotenv()

# Secret key for JWT encoding/decoding
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM")
TABLE_NAMES = declare_constants.GET_TABLE_NAMES()
db_path = os.getenv("DB_PATH")
con = duckdb.connect(db_path,read_only=True)

def authenticate_user(username: str, password: str):
    result = con.execute(f"SELECT password_hash FROM {TABLE_NAMES['USERS']} WHERE username = ?", [username]).fetchone()
    if result and result[0] == hash_password(password):
        return User(username=username)
    return None

def create_token(username: str):
    return jwt.encode({"sub": username}, SECRET_KEY, algorithm=ALGORITHM)