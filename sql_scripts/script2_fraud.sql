insert into deaise.voal_rep_fraud
with all_t as (select vsah.client, 
		concat(vsch.last_name || ' ' || vsch.first_name || ' ' || vsch.patronymic) fio,
		vsch.phone,
		vsah.account_num,
		vsc.card_num,
		vsah.valid_to,
		vdft.amt,
		vdft.trans_date trans_dt,
		vdft.oper_result,
		vsth.terminal_id,
		vsth.terminal_address,
		vsth.terminal_city,
		vsch.passport_num passport,
		vsch.passport_valid_to
from deaise.voal_dwh_dim_clients_hist vsch
left join deaise.voal_dwh_dim_accounts_hist vsah
	on vsch.client_id = vsah.client
left join deaise.voal_dwh_dim_cards_hist vsc
	on vsah.account_num = vsc.account_num
left join deaise.voal_dwh_fact_transactions vdft
	on vdft.card_num = trim(vsc.card_num)
left join deaise.voal_dwh_dim_terminals_hist vsth
	on vsth.terminal_id = vdft.terminal and vdft.trans_date between vsth.effective_from and vsth.effective_to
order by vsah.client, vsah.valid_to),
t_2 as (select trans_dt, passport, fio, phone, 2 event_type, trans_dt::date report_dt from all_t
		where account_num in (select account_num from deaise.voal_dwh_dim_accounts_hist where valid_to < trans_dt))
select * from t_2 where trans_dt not in (select event_dt from deaise.voal_rep_fraud);