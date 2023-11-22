delete from deaise.voal_stg_terminals_del;

-- 1. Захват в стейджинг ключей из источника полным срезом для вычисления удалений.
insert into deaise.voal_stg_terminals_del ( terminal_id )
select terminal_id from deaise.voal_stg_terminals;

-- 2. Загрузка в приемник "вставок" на источнике (формат SCD2)
insert into deaise.voal_dwh_dim_terminals_hist ( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
select 
    stg.terminal_id,
    stg.terminal_type,
    stg.terminal_city,
    stg.terminal_address,
    stg.create_dt effective_from,
	cast(coalesce(lead(stg.update_dt) over (partition by stg.terminal_id  order by stg.update_dt) - interval '30 second',
to_date('9999-12-31','YYYY-MM-DD')) as timestamp) effective_to,
	'N' 
from deaise.voal_stg_terminals stg
 	left join deaise.voal_dwh_dim_terminals_hist tgt
 	on stg.terminal_id = tgt.terminal_id 
where tgt.terminal_id is null;

-- 3. Обновление в приемнике "обновлений" на источнике (формат SCD2).
insert into deaise.voal_dwh_dim_terminals_hist ( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
select 
	stg.terminal_id,
	stg.terminal_type,
	stg.terminal_city,
	stg.terminal_address,
	stg.create_dt,
	to_date('9999-12-31','YYYY-MM-DD') as timestamp ,
	'N' 
from deaise.voal_stg_terminals stg 
    inner join deaise.voal_dwh_dim_terminals_hist tgt
    on stg.terminal_id = tgt.terminal_id
    and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
where ( stg.terminal_type <> tgt.terminal_type or (stg.terminal_type is null and tgt.terminal_type is not null) or (stg.terminal_type is not null and tgt.terminal_type is null)
	or stg.terminal_city <> tgt.terminal_city or (stg.terminal_city is null and tgt.terminal_city is not null) or (stg.terminal_city is not null and tgt.terminal_city is null)
	or stg.terminal_address <> tgt.terminal_address or (stg.terminal_address is null and tgt.terminal_address is not null) or (stg.terminal_address is not null and tgt.terminal_address is null))
    or tgt.deleted_flg = 'Y';

update deaise.voal_dwh_dim_terminals_hist tgt 
   set effective_to = tmp.create_dt - interval '1 second'
from (
	select 
		stg.terminal_id,
		stg.terminal_type,
		stg.terminal_city,
		stg.terminal_address,
		stg.create_dt
	from deaise.voal_stg_terminals stg 
	inner join deaise.voal_dwh_dim_terminals_hist tgt
	  	on stg.terminal_id = tgt.terminal_id
	 	and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	where ( stg.terminal_type <> tgt.terminal_type or (stg.terminal_type is null and tgt.terminal_type is not null) or (stg.terminal_type is not null and tgt.terminal_type is null)
	or stg.terminal_city <> tgt.terminal_city or (stg.terminal_city is null and tgt.terminal_city is not null) or (stg.terminal_city is not null and tgt.terminal_city is null)
	or stg.terminal_address <> tgt.terminal_address or (stg.terminal_address is null and tgt.terminal_address is not null) or (stg.terminal_address is not null and tgt.terminal_address is null))
        or tgt.deleted_flg = 'Y') tmp
where tgt.terminal_id = tmp.terminal_id
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and ( tmp.terminal_type <> tgt.terminal_type or (tmp.terminal_type is null and tgt.terminal_type is not null) or (tmp.terminal_type is not null and tgt.terminal_type is null)
	or tmp.terminal_city <> tgt.terminal_city or (tmp.terminal_city is null and tgt.terminal_city is not null) or (tmp.terminal_city is not null and tgt.terminal_city is null)
	or tmp.terminal_address <> tgt.terminal_address or (tmp.terminal_address is null and tgt.terminal_address is not null) or (tmp.terminal_address is not null and tgt.terminal_address is null))
    or tgt.deleted_flg = 'Y';

-- 4. Обработка удалений в приемнике (формат SCD2).
insert into deaise.voal_dwh_dim_terminals_hist ( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
select 
    tgt.terminal_id,
	tgt.terminal_type,
	tgt.terminal_city,
	tgt.terminal_address,
	now() start_dt,
	to_date('9999-12-31','YYYY-MM-DD') end_dt,
	'Y' deleted_flg
from deaise.voal_dwh_dim_terminals_hist tgt 
left join deaise.voal_stg_terminals_del stg
  	on stg.terminal_id = tgt.terminal_id
where stg.terminal_id is null
  	and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  	and tgt.deleted_flg = 'N';

update deaise.voal_dwh_dim_terminals_hist tgt 
   set effective_to = now() - interval '1 second'
where tgt.terminal_id in (
	select 
		tgt.terminal_id
	from deaise.voal_dwh_dim_terminals_hist tgt 
	left join deaise.voal_stg_terminals_del stg
	  	on stg.terminal_id = tgt.terminal_id
	where stg.terminal_id is null
	  	and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	  	and deleted_flg = 'N')
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and deleted_flg = 'N';

-- 5. Обновление метаданных.

update deaise.voal_meta_stg
  set max_update_dt = coalesce((select max(update_dt) from deaise.voal_stg_terminals), now())
where schema_name = 'info' and table_name = 'terminals';