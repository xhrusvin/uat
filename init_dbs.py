# Optional helper: create sample DBs for some countries
from db_config import init_country_db_if_missing
for c in ['IN','US','UK']:
    init_country_db_if_missing(c)
print('Initialized sample country DBs: IN, US, UK')
