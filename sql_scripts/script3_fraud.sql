insert into deaise.voal_rep_fraud
with t3 as (
	select  trans_id,
		    client,
		    trans_date,
		    coalesce (lag(trans_date) over(partition by client order by trans_date), trans_date)  prev_dt,
		    coalesce (lead(trans_date) over(partition by client order by trans_date), trans_date)  next_dt,
		    terminal_city,
		    coalesce (lag(terminal_city) over(partition by client order by trans_date), terminal_city)  prev_city,
		    coalesce (lead(terminal_city) over(partition by client order by trans_date), terminal_city)  next_city
	from deaise.voal_dwh_fact_transactions vdft
	left join deaise.voal_dwh_dim_terminals_hist vsth 	on vsth.terminal_id = vdft.terminal and vdft.trans_date between vsth.effective_from and vsth.effective_to
	left join deaise.voal_dwh_dim_cards_hist vsc on vdft.card_num = trim(vsc.card_num)
	left join deaise.voal_dwh_dim_accounts_hist vsah on vsah.account_num = vsc.account_num
	          )
select trans_date::time as event_dt,
       passport_num as passport,
       concat(last_name || ' ' || first_name || ' ' || patronymic) fio,
       phone,
       3 as event_type,
       (select max(trans_date::date) from deaise.voal_dwh_fact_transactions) report_dt
from t3
left join deaise.voal_dwh_dim_clients_hist cl on t3.client = cl.client_id
where (prev_city <> terminal_city and (trans_date - prev_dt) < interval '60 minute') and prev_city = next_city