insert into deaise.voal_dwh_fact_transactions 
	select
		stg.trans_id,
		stg.trans_date,
		stg.card_num,
		stg.oper_type,
		stg.amt,
		stg.oper_result,
		stg.terminal
	from  deaise.voal_stg_transactions stg