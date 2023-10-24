insert into deaise.voal_dwh_fact_passport_blacklist 
	select
		stg.passport_num,
		stg.entry_dt
	from  deaise.voal_stg_blacklist stg
	    left join deaise.voal_dwh_fact_passport_blacklist tgt
	    on stg.passport_num = tgt.passport_num
	where tgt.passport_num is null;