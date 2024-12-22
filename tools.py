from datetime import datetime

import duckdb
import dotenv
import os
import declare_constants
import logging
from hashing import hash_query, decode_answer
import visualization
# ... existing code ...
from pydantic import BaseModel
from typing import List, Dict

class ContextData(BaseModel):
    data: List[Dict]  # List of dictionaries
    source: str


logging.basicConfig(level=declare_constants.get_log_level())
logger = logging.getLogger(__name__)

dotenv.load_dotenv()
db_path = os.getenv("DB_PATH")
con = duckdb.connect(db_path,read_only=True)

TABLE_NAMES = declare_constants.GET_TABLE_NAMES()

def get_supplier_of_mg(con: duckdb.DuckDBPyConnection, hashed_mg_id: str) -> ContextData:
    result = ContextData(data=[], source="get_supplier_of_mg")
    
    df = con.sql(f"""
                 select a.hashed_mg_id as mg_id, c.hashed_mg_id as supplier, c.country as supplier_country, c.is_subsid as supplier_is_subsid ,
                 b.amount as amount, b.date as date, b.description as description
                 from {TABLE_NAMES['PARTY_BELONG_TO_MG']} a, {TABLE_NAMES['PAYMENT']} b , {TABLE_NAMES['PARTY_BELONG_TO_MG']} c
                 where a.hashed_mg_id = '{hashed_mg_id}' 
                 and a.party_id = b.payee_id
                 and b.payer_id = c.party_id
                 """).to_df()
    if len(df) > 0:
        logger.debug(f"Getting supplier of mg: {hashed_mg_id}")
        logger.debug(df[['mg_id','supplier']])
        result.data = df.to_dict(orient='records')
        logger.debug(f"Tooling Result: {result}")
        return result
    return None

def get_buyer_of_mg(con: duckdb.DuckDBPyConnection, hashed_mg_id: str) -> ContextData:
    result = ContextData(data=[], source="get_buyer_of_mg")
    df = con.sql(f"""
                 select a.hashed_mg_id as mg_id, c.hashed_mg_id as buyer, c.country as buyer_country, c.is_subsid as buyer_is_subsid ,
                 b.amount as amount, b.date as date, b.description as description
                 from {TABLE_NAMES['PARTY_BELONG_TO_MG']} a, {TABLE_NAMES['PAYMENT']} b , {TABLE_NAMES['PARTY_BELONG_TO_MG']} c
                 where a.hashed_mg_id = '{hashed_mg_id}' 
                 and a.party_id = b.payer_id
                 and b.payee_id = c.party_id
                 """).to_df()
    if len(df) > 0:
        logger.debug(f"Getting buyer of mg: {hashed_mg_id}")
        logger.debug(df)
        result.data = df.to_dict(orient='records')
        logger.debug(f"Tooling Result: {result}")
        return result
    return None



