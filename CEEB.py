import pandas as pd
import pyodbc


def connect_to_db():
    cnxn = pyodbc.connect('DSN=CX-STAGE')
    cursor = cnxn.cursor()

    return cursor


def read_secondary_CEEB_list():
    file_name = 'C:/Users/aweaver/Desktop/ATP.I840.AIFULL.20190529122101.txt'

    w_specification = [6, 30, 30, 30, 16, 4, 10, 27, 1, 6, 1, 1]
    columns = ['SECONDARY SCHOOL CODE', 'SCHOOL NAME', 'ADDRESS LINE #1', 'ADDRESS LINE #2', 'CITY',
               'STATE/PROVINCE', 'ZIP-5/POSTAL CODE', 'COUNTRY NAME', 'ADDRESS IND.', 'CHANGE DATE',
               'CHANGE CODE', 'SCHOOL TYPE']

    df = pd.read_fwf(file_name, widths=w_specification, header=None, index_col=False, names=columns)

    return df

def read_primary_CEEB_list():
    file_name = 'C:/Users/aweaver/Desktop/ATP.I864.DIFULL.C.20190530115101.txt'

    w_specification = [6, 30, 30, 30, 16, 4, 10, 1, 6, 1, 1, 5]
    columns = ['COLLEGE CODE', 'COLLEGE NAME', 'ADDRESS LINE #1', 'ADDRESS LINE #2', 'CITY',
               'STATE/PROVINCE', 'ZIP-5/POSTAL CODE', 'ADDRESS IND.', 'CHANGE DATE',
               'CHANGE CODE', 'PROGRAM IN YEARS', 'STATUS OF INST.']

    df = pd.read_fwf(file_name, widths=w_specification, header=None, index_col=False, names=columns)

    return df


df_primary = read_primary_CEEB_list()
df_secondary = read_secondary_CEEB_list()
cursor = connect_to_db()
sql = 'select * from SCH_REC as school \
inner join id_rec as id \
on school.id = id.id'
rows = cursor.execute(sql)
count=0
total= 0
for row in rows:
    total += 1
    # check for direct ceeb match
    ceeb_match = df_primary.loc[df_primary['COLLEGE CODE'] == row.ceeb]
    if ceeb_match.empty:
        # try secondary list
        ceeb_match = df_secondary.loc[df_secondary['SECONDARY SCHOOL CODE'] == row.ceeb]
        if ceeb_match.empty:
            count += 1

print('final percent ', (count/total) * 100)