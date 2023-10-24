-- Создание стейджинговых таблиц

create table deaise.voal_stg_blacklist (
	entry_dt date,
	passport_num varchar(15)
);

create table deaise.voal_stg_transactions (
	trans_id varchar(20),
	trans_date timestamp(0),
	amt varchar,
	card_num varchar(20),
	oper_type varchar(10),
	oper_result varchar(10),
	terminal varchar(10)
);

create table deaise.voal_stg_cards (
	card_num varchar(20),
	account_num varchar(20),
    create_dt timestamp(0),
	update_dt timestamp(0)
);

create table deaise.voal_stg_accounts (
	account_num varchar(20),
	valid_to timestamp(0),
	client varchar(10),
    create_dt timestamp(0),
	update_dt timestamp(0)
);

create table deaise.voal_stg_clients (
	client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(20),
    create_dt timestamp(0),
	update_dt timestamp(0)
);

create table deaise.voal_stg_terminals (
	terminal_id varchar(10),
	terminal_type varchar(4),
	terminal_city varchar(20),
	terminal_address varchar(100),
	create_dt timestamp(0),
	update_dt timestamp(0) default NULL
);

-- Фиксация удаленных данных

create table deaise.voal_stg_clients_del (
    client_id varchar(10)
);

create table deaise.voal_stg_cards_del (
	card_num varchar(20)   
);

create table deaise.voal_stg_accounts_del (
	account_num varchar(20)
);

create table deaise.voal_stg_terminals_del (
	terminal_id varchar(10)
);

create table deaise.voal_meta_stg(
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);

insert into deaise.voal_meta_stg ( schema_name, table_name, max_update_dt )
values( 'info','clients', to_timestamp('1900-01-01','YYYY-MM-DD')),
	   ('info','cards', to_timestamp('1900-01-01','YYYY-MM-DD')),
	   ('info','accounts', to_timestamp('1900-01-01','YYYY-MM-DD')),
	   ('info','terminals', to_timestamp('1900-01-01','YYYY-MM-DD')
);

-- Создание фактовых таблиц

create table deaise.voal_dwh_fact_passport_blacklist (
	passport_num varchar(15),
	entry_dt date
);

create table deaise.voal_dwh_fact_transactions (
	trans_id varchar(20),
	trans_date timestamp(0),
	card_num varchar(20),
	oper_type varchar(10),
	amt varchar,
	oper_result varchar(10),
	terminal varchar(10)
);

-- Таблицы изменений в SCD2

create table deaise.voal_stg_cards_hist (
	card_num varchar(20),
	account_num varchar(20),
    effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg varchar(1)
);

create table deaise.voal_stg_accounts_hist (
	account_num varchar(20),
	valid_to timestamp(0),
	client varchar(10),
    effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg varchar(1)
);

create table deaise.voal_stg_clients_hist (
	client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(20),
    effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg varchar(1)
);

create table deaise.voal_stg_terminals_hist (
	terminal_id varchar(10),
	terminal_type varchar(4),
	terminal_city varchar(20),
	terminal_address varchar(100),
    effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg varchar(1)
);



-- Таблицы с отчетами

create table deaise.voal_rep_fraud (
	event_dt timestamp(0),
	passport varchar(15),
	fio varchar(60),
	phone varchar(20),
	event_type numeric,
	report_dt date
);


-- select * from deaise.voal_stg_passport_blacklist;
-- select * from deaise.voal_stg_transactions;
-- select * from deaise.voal_stg_cards;
-- select * from deaise.voal_stg_accounts;
-- select * from deaise.voal_stg_clients;
-- select * from deaise.voal_stg_terminals;
-- select * from deaise.voal_stg_clients_del;
-- select * from deaise.voal_stg_cards_del;
-- select * from deaise.voal_stg_accounts_del;
-- select * from deaise.voal_meta_stg;
-- select * from deaise.voal_dwh_fact_passport_blacklist;
-- select * from deaise.voal_dwh_fact_transactions;
-- select * from deaise.voal_stg_cards_hist;
-- select * from deaise.voal_stg_accounts_hist;
-- select * from deaise.voal_stg_clients_hist;
-- select * from deaise.voal_stg_terminals_hist;

--  drop table deaise.voal_stg_blacklist;
--  drop table deaise.voal_stg_transactions;
--  drop table deaise.voal_stg_cards;
--  drop table deaise.voal_stg_accounts;
--  drop table deaise.voal_stg_clients;
--  drop table deaise.voal_stg_terminals;
--  drop table deaise.voal_stg_clients_del;
--  drop table deaise.voal_stg_cards_del;
--  drop table deaise.voal_stg_accounts_del;
--  drop table deaise.voal_stg_terminals_del;
--  drop table deaise.voal_meta_stg;
--  drop table deaise.voal_dwh_fact_passport_blacklist;
--  drop table deaise.voal_dwh_fact_transactions;
--  drop table deaise.voal_stg_cards_hist;
--  drop table deaise.voal_stg_accounts_hist;
--  drop table deaise.voal_stg_clients_hist;
--  drop table deaise.voal_stg_terminals_hist;
--  drop table deaise.voal_rep_fraud;

-- delete from deaise.voal_stg_passport_blacklist;
-- delete from deaise.voal_stg_transactions;
-- delete from deaise.voal_stg_cards;
-- delete from deaise.voal_stg_accounts;
-- delete from deaise.voal_stg_clients;
-- delete from deaise.voal_stg_terminals;
-- delete from deaise.voal_stg_clients_del;
-- delete from deaise.voal_stg_cards_del;
-- delete from deaise.voal_stg_accounts_del;
-- delete from deaise.voal_meta_stg;
-- delete from deaise.voal_dwh_fact_passport_blacklist;
-- delete from deaise.voal_dwh_fact_transactions;
-- delete from deaise.voal_stg_cards_hist;
-- delete from deaise.voal_stg_accounts_hist;
-- delete from deaise.voal_stg_clients_hist;
-- delete from deaise.voal_stg_terminals_hist;