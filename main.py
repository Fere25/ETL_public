#!/usr/bin/python3
import psycopg2
import pandas as pd
import os
from datetime import datetime

print('Создание подключений')

# Создание подключения к PostgreSQL
conn_src = psycopg2.connect(*)

print('Подключились к bank')

conn_dwh = psycopg2.connect(*)


print('Подключились к edu')

# Функция выполнения SQL скриптов
def execute_sql_from_file(file_name):
	with open(file_name, 'r') as f:
		sql_script = f.read()
	sql_statements = sql_script.split(';')
	sql_statements = [sql.strip() for sql in sql_statements if sql.strip()]
	for sql in sql_statements:
		cursor_dwh.execute(sql)
		conn_dwh.commit()


directory = '/home/deaise/voal/project'
directory_backup = '/home/deaise/voal/project/archive'
extension = '.txt'
files = [f for f in os.listdir(directory) if f.endswith(extension)]
for filename in files:
    date_rep = ''.join(filter(str.isdigit, filename))


# Отключение автокоммита
conn_src.autocommit = False
conn_dwh.autocommit = False

# Создание курсора
cursor_src = conn_src.cursor()
cursor_dwh = conn_dwh.cursor()

## Очистка Stage
print('0. Очистка stage')

cursor_dwh.execute( "delete from deaise.voal_stg_blacklist" )
cursor_dwh.execute( "delete from deaise.voal_stg_transactions" )
cursor_dwh.execute( "delete from deaise.voal_stg_cards" )
cursor_dwh.execute( "delete from deaise.voal_stg_accounts" )
cursor_dwh.execute( "delete from deaise.voal_stg_clients" )
cursor_dwh.execute( "delete from deaise.voal_stg_terminals" )

## Загрузка данных в Stage
print('1. Загрузка в stage')

## voal_stg_passport_blacklist
print('1.1. voal_stg_blacklist')

for filename in os.listdir(directory):
    if filename.endswith('.xlsx') and "passport_blacklist_" in filename:
        df = pd.read_excel(os.path.join(directory, filename), sheet_name='blacklist', header=0, index_col=None)
        source_path = os.path.join(directory, filename)
        destination_filename = filename + '.backup'
        destination_path = os.path.join(directory_backup, destination_filename)
        
        os.rename(source_path, destination_path)
        os.replace(os.path.join(directory_backup, destination_filename), destination_path)
cursor_dwh.executemany( "INSERT INTO deaise.voal_stg_blacklist( entry_dt, passport_num ) VALUES( %s, %s )", df.values.tolist() )

## voal_stg_transactions
print('1.2. voal_stg_transactions')

for filename in os.listdir(directory):
    if filename.endswith('.txt') and "transactions_" in filename:
        df = pd.read_table(os.path.join(directory, filename), sep=';', header=0, index_col=None)
        source_path = os.path.join(directory, filename)
        destination_filename = filename + '.backup'
        destination_path = os.path.join(directory_backup, destination_filename)
        
        os.rename(source_path, destination_path)
        os.replace(os.path.join(directory_backup, destination_filename), destination_path)
cursor_dwh.executemany( """INSERT INTO deaise.voal_stg_transactions( 
                            trans_id,
                            trans_date,
                            amt,
                            card_num,
                            oper_type,
                            oper_result,
                            terminal ) 
                        VALUES( %s, %s, %s, trim(%s), %s, %s, %s )""", df.values.tolist() )

## voal_stg_terminals
print('1.3. voal_stg_terminals')

for filename in os.listdir(directory):
    if filename.endswith('.xlsx') and "terminals_" in filename:
        df = pd.read_excel(os.path.join(directory, filename), sheet_name='terminals', header=0, index_col=None)
        source_path = os.path.join(directory, filename)
        destination_filename = filename + '.backup'
        destination_path = os.path.join(directory_backup, destination_filename)
        
        os.rename(source_path, destination_path)
        os.replace(os.path.join(directory_backup, destination_filename), destination_path)
default_date = datetime.strptime(date_rep, '%d%m%Y').strftime('%Y-%m-%d')
df['create_dt'] = default_date
cursor_dwh.executemany( """INSERT INTO deaise.voal_stg_terminals( 
                            terminal_id,
                            terminal_type,
                            terminal_city,
                            terminal_address,
					        create_dt)
                        VALUES( %s, %s, %s, %s, %s )""", df.values.tolist() )


## voal_stg_cards
print('1.4. voal_stg_cards')

cursor_src.execute( """ select 
                            card_num,
                            account,
                            create_dt,
                            update_dt
                        from info.cards """)

records = cursor_src.fetchall()
df = pd.DataFrame( records )

cursor_dwh.executemany( """insert into deaise.voal_stg_cards(
                            card_num,
                            account_num,
                            create_dt,
                            update_dt)
                       values( %s, %s, %s, %s )""", df.values.tolist() )

## voal_stg_accounts
print('1.5. voal_stg_accounts')

cursor_src.execute( """ select 
                            account,
                            valid_to,
                            client,
                            create_dt,
                            update_dt
                        from info.accounts """)

records = cursor_src.fetchall()
df = pd.DataFrame( records )

cursor_dwh.executemany( """insert into deaise.voal_stg_accounts(
                            account_num,
                            valid_to,
                            client,
                            create_dt,
                            update_dt)
                        values( %s, %s, %s, %s, %s )""", df.values.tolist() ) 
                       
## voal_stg_clients
print('1.6. voal_stg_clients')

cursor_src.execute( """ select 
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            create_dt,
                            update_dt 
                        from info.clients """)

records = cursor_src.fetchall()
df = pd.DataFrame( records )

cursor_dwh.executemany( """insert into deaise.voal_stg_clients(
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            create_dt,
                            update_dt )
                        values( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )""", df.values.tolist() ) 



## Загрузка данных в Детальный слой DDS

print('2.1 Загрузка данных в Детальный слой DDS passport_blacklist')
execute_sql_from_file('./sql_scripts/passport_blacklist.sql')

print('2.2 Загрузка данных в Детальный слой DDS transactions')
execute_sql_from_file('./sql_scripts/transactions.sql')

print('2.3 Загрузка данных в Детальный слой DDS terminals')
execute_sql_from_file('./sql_scripts/terminals.sql')

print('2.4 Загрузка данных в Детальный слой DDS accounts')
execute_sql_from_file('./sql_scripts/accounts.sql')

print('2.5 Загрузка данных в Детальный слой DDS cards')
execute_sql_from_file('./sql_scripts/cards.sql')

print('2.6 Загрузка данных в Детальный слой DDS clients')
execute_sql_from_file('./sql_scripts/clients.sql')


print('3 Загрузка данных в Fraud')
execute_sql_from_file('./sql_scripts/fraud.sql')

####################################################################################
# Закрываем соединение
conn_dwh.commit()
print('99. Закрытие подключений')
cursor_src.close()
cursor_dwh.close()
conn_src.close()
conn_dwh.close()





