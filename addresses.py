import pyodbc
import requests
import json

cnxn = pyodbc.connect('DSN=CX-STAGE')
cursor = cnxn.cursor()
bad_records = 0
total_recs = 0

cursor = cnxn.cursor()
# UNCOMMENT THE BELOW FOR ID_REC
#sql = 'SELECT NVL(addr_line1,"") addr_line1, NVL(addr_line2,"") addr_line2, NVL(addr_line3,"") addr_line3, NVL(city,"") \
# city, NVL(st,"") st, NVL(zip,"") zip FROM ID_REC master INNER JOIN NHSTUIDS_REC filter ON master.id = filter.id'

sql = "SELECT NVL(line1,'') addr_line1, NVL(line2,'') addr_line2, NVL(line3,'') addr_line3, NVL(city,'') \
city, NVL(st,'') st, NVL(zip,'') zip FROM AA_REC master \
INNER JOIN NHSTUIDS_REC filter ON master.id = filter.id \
where master.end_date is NULL AND \
      master.aa IN ('CONF', 'DADR', 'MAIL', 'PAY', 'PERM', 'PPER', 'PPMA', 'PPRM', 'PREV', 'WORK')"
rows = cursor.execute(sql)

for row in rows:
    total_recs = total_recs + 1
    url = "https://docker.northampton.edu:443/validation/addressvalidation/validate"

    payload = {"line_1": row.addr_line1,
               "line_2": row.addr_line2,
               "line_3": row.addr_line3,
               "city": row.city,
               "state": row.st,
               "zip": row.zip}
    payload = json.dumps(payload)
    headers = {
        'Content-Type': "application/json",
        'Authorization': "Basic QURNSU46VURyeUZVbWZaSjlyWXFI",
        'cache-control': "no-cache"
    }

    response = requests.request("POST", url, data=payload, headers=headers)
    response_data = response.json()
    if response_data.get('status') != "Valid":
        bad_records += 1


print((bad_records/total_recs)*100)
