import pandas as pd
import warnings
from sqlalchemy import create_engine, text
import sqlalchemy.engine
from trino.auth import OAuth2Authentication

engine = 

class TrinoDBAdapter:
    def __init__(self,username,host,port=443,verify=False)
        self.engine = create_engine(
    "trino://{your username here}@trino.feasibility.sail.pk.serp.ac.uk:443",
    connect_args={
        "auth": OAuth2Authentication(),
        "http_scheme": "https",
        "verify": False
    }
)