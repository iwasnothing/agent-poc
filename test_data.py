from datetime import datetime
import random
import string
import uuid
import duckdb
import hashlib
import dotenv
import os
import declare_constants
import logging
from user import hash_password
dotenv.load_dotenv()



TABLE_NAMES = declare_constants.GET_TABLE_NAMES()
logging.basicConfig(level=declare_constants.get_log_level())
logger = logging.getLogger(__name__)

def generate_random_string(length: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_random_mg_id() -> str:
    return f"MG {generate_random_string(3)}"

def hash_mg_id(mg_id: str) -> str:
    return hashlib.sha256(mg_id.encode()).hexdigest()

def create_MG_HASH_MAP_table(con,num_mg):
    con.sql(f"drop table if exists {TABLE_NAMES['MG_HASH_MAP']}")
    con.sql(f"CREATE TABLE if not exists {TABLE_NAMES['MG_HASH_MAP']} (mg_id TEXT PRIMARY KEY, hash TEXT)")
    con.sql(f"DELETE FROM {TABLE_NAMES['MG_HASH_MAP']}")
    
    for i in range(num_mg):
        mg_id = generate_random_mg_id()
        hash = hash_mg_id(mg_id)
        con.sql(f"INSERT INTO {TABLE_NAMES['MG_HASH_MAP']} VALUES ('{mg_id}', '{hash}')")
    df = con.sql(f"select * from {TABLE_NAMES['MG_HASH_MAP']}").to_df()
    logger.debug(df)

def create_PARTY_table(con,num_mg):
    con.sql(f"drop table if exists {TABLE_NAMES['PARTY_BELONG_TO_MG']}")
    con.sql(f"CREATE TABLE if not exists {TABLE_NAMES['PARTY_BELONG_TO_MG']} (party_id TEXT PRIMARY KEY, hashed_mg_id TEXT, country TEXT, is_subsid BOOLEAN)")
    con.sql(f"DELETE FROM {TABLE_NAMES['PARTY_BELONG_TO_MG']}")
    df = con.sql(f"select * from {TABLE_NAMES['MG_HASH_MAP']}").to_df()
    for index, row in df.iterrows():
        mg_id = row['mg_id']
        hashed_mg_id = row['hash']
        country = random.choice(["HK", "UK"])
        is_subsid = random.choice([True, False])
        party_id = uuid.uuid4()
        con.sql(f"INSERT INTO {TABLE_NAMES['PARTY_BELONG_TO_MG']} VALUES ('{party_id}', '{hashed_mg_id}', '{country}', '{is_subsid}')")
    df = con.sql(f"select * from {TABLE_NAMES['PARTY_BELONG_TO_MG']}").to_df()
    logger.debug(df)

def create_PAYMENT_table(con,num_mg): 
    con.sql(f"drop table if exists {TABLE_NAMES['PAYMENT']}")
    con.sql(f"CREATE TABLE if not exists {TABLE_NAMES['PAYMENT']} (payment_id TEXT PRIMARY KEY, payer_id TEXT, payee_id TEXT, amount FLOAT, date TEXT, description TEXT)")
    con.sql(f"DELETE FROM {TABLE_NAMES['PAYMENT']}")
    df = con.sql(f"select * from {TABLE_NAMES['PARTY_BELONG_TO_MG']}").to_df()
    for index1, row1 in df.iterrows():
        for index2, row2 in df.iterrows():
            if row1['party_id'] != row2['party_id']:
                payer_id = row1['party_id']
                payee_id = row2['party_id']
                amount = random.randint(100, 1000)
                date = datetime.now().strftime("%Y-%m-%d")
                description = f"Payment from {payer_id} to {payee_id}"
                payment_id = uuid.uuid4()
                con.sql(f"INSERT INTO {TABLE_NAMES['PAYMENT']} VALUES ('{payment_id}', '{payer_id}', '{payee_id}', '{amount}', '{date}', '{description}')")
    df = con.sql(f"select * from {TABLE_NAMES['PAYMENT']}").to_df()
    logger.debug(df)   

# Initialize DuckDB connection


def create_USERS_table(con):
    con.sql(f"drop table if exists {TABLE_NAMES['USERS']}")
    con.sql(f"CREATE TABLE if not exists {TABLE_NAMES['USERS']} (username VARCHAR PRIMARY KEY, password_hash VARCHAR)")
    con.sql(f"DELETE FROM {TABLE_NAMES['USERS']}")
    for i in range(2):
        username = f"user_{i}"
        password_hash = hash_password(f"password_{i}")
        con.sql(f"INSERT INTO {TABLE_NAMES['USERS']} VALUES ('{username}', '{password_hash}')")
    df = con.sql(f"select * from users").to_df()
    logger.debug(df)


def create_test_data(con,num_mg):
    create_MG_HASH_MAP_table(con,num_mg )
    create_PARTY_table(con,num_mg)
    create_PAYMENT_table(con,num_mg)
    create_USERS_table(con)
    con.close()