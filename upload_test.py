import pandas as pd
from app.utils.db_engine import get_engine

engine = get_engine()

df.to_sql("rawsellers_test", con=engine, if_exists='replace', index=False)
