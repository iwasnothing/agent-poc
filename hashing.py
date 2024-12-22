from datetime import datetime
import duckdb
import dotenv
import os
import declare_constants
import logging

logging.basicConfig(level=declare_constants.get_log_level())
logger = logging.getLogger(__name__)

dotenv.load_dotenv()
db_path = os.getenv("DB_PATH")
con = duckdb.connect(db_path,read_only=True)

TABLE_NAMES = declare_constants.GET_TABLE_NAMES()

def hash_query(query: str) -> str:
    mapping = {}
    tokens = query.split()
    logger.debug(f"tokens: {tokens}")
    n = len(tokens)
    logger.debug(f"n: {n}")
    for i in range(n):
        for j in range(i+1, n+1):
            logger.debug(f"i: {i}, j: {j}")
            sub_query = " ".join(tokens[i:j])
            logger.debug(f"sub_query: {sub_query}")
            if not sub_query[-1].isalnum():
                sub_query = sub_query[:-1]
            logger.debug(f"sub_query: {sub_query}")
            df = con.sql(f"select hash from {TABLE_NAMES['MG_HASH_MAP']} where mg_id = '{sub_query}'").to_df()
            if len(df) > 0:
                hash_value = df['hash'].values[0]
                logger.debug(f"hash_value: {hash_value}")
                mapping[sub_query] = hash_value

    for key, value in mapping.items():
        query = query.replace(key, "{hashed_mg_id: " +value+" }")
    return query

def decode_answer(answer: str) -> str:
    tokens = answer.split()
    decoded_answer = []
    for token in tokens:
        original_token = token
        token = token.replace("\'", "").replace("\"", "")
        if len(token) > 0:
            logger.debug(f"token: {token}")
            if not token[0].isalnum():
                token = token[1:]
            if len(token) > 0 and not token[-1].isalnum():
                token = token[:-1]
            
            logger.debug(f"trimmed token: {token}")
        df = con.sql(f"select mg_id from {TABLE_NAMES['MG_HASH_MAP']} where hash = '{token}'").to_df()
        if len(df) > 0:
            logger.debug(f"df: {df}")
            decoded_mg_id = df['mg_id'].values[0]
            decoded_token = original_token.replace(token, decoded_mg_id)
            decoded_answer.append(decoded_token)
        else:
            decoded_answer.append(original_token)
    return " ".join(decoded_answer)

if __name__ == "__main__":
    query = "what is the supplier of MG IWO?"
    hashed_query = hash_query(query)
    logger.debug(f"hashed_query: {hashed_query}")
    decoded_answer = decode_answer(hashed_query)
    logger.debug(f"decoded_answer: {decoded_answer}")
