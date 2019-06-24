import pyodbc
import requests

cnxn = pyodbc.connect('DSN=CX-STAGE')
cursor = cnxn.cursor()
bad_records = 0
total_recs = 0

cursor = cnxn.cursor()
sql = "SELECT phone FROM ID_REC as master \
INNER JOIN NHSTUIDS_REC as filter \
ON master.id = filter.id"
rows = cursor.execute(sql)

for row in rows:
    total_recs = total_recs + 1
    if row.phone is None:
        bad_records += 1
        continue
    row.phone = row.phone.strip(' ')
    if len(row.phone) == 0:
        bad_records += 1
        continue
    row.phone = '+1' + row.phone
    row.phone = row.phone.replace('-', '')
    url = f"https://docker.northampton.edu:443/validation/phonenumbervalidation/validate/{row.phone}"

    headers = {
        'Content-Type': "application/json",
        'Authorization': "Basic QURNSU46S1BXUTN1MXdCaDk5c2s=",
        'accept-encoding': "gzip, deflate",
        'Connection': "keep-alive",
        'cache-control': "no-cache"
    }

    response = requests.request("GET", url, headers=headers)
    if response.status_code != 200:
        continue
    response = response.json()
    if response['valid'] == 'False':
        bad_records += 1
        continue


print('total_recs= ', total_recs)
print((bad_records/total_recs)*100)
