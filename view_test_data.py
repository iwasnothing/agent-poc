import duckdb
import dotenv
import os
import declare_constants

dotenv.load_dotenv()
db_path = os.getenv("DB_PATH")
con = duckdb.connect(db_path,read_only=True)

TABLE_NAMES = declare_constants.GET_TABLE_NAMES()

con.sql(f"select * from {TABLE_NAMES['PAYMENT']}").show()
con.sql(f"select * from {TABLE_NAMES['PARTY_BELONG_TO_MG']}").show()
con.sql(f"select * from {TABLE_NAMES['MG_HASH_MAP']}").show()
df = con.sql(f"""
                 select a.hashed_mg_id as mg_id, c.hashed_mg_id as supplier, c.country as supplier_country, c.is_subsid as supplier_is_subsid ,
                 b.amount as amount, b.date as date, b.description as description
                 from {TABLE_NAMES['PARTY_BELONG_TO_MG']} a, {TABLE_NAMES['PAYMENT']} b , {TABLE_NAMES['PARTY_BELONG_TO_MG']} c
                 where a.party_id = b.payee_id
                 and b.payer_id = c.party_id
                 """).to_df()
print(df[['mg_id','supplier']])