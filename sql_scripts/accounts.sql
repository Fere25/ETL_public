delete from deaise.voal_stg_accounts_del;

-- 1. Захват в стейджинг ключей из источника полным срезом для вычисления удалений.
insert into deaise.voal_stg_accounts_del ( account_num )
select account_num from deaise.voal_stg_accounts;

-- 2. Загрузка в приемник "вставок" на источнике (формат SCD2)
insert into deaise.voal_stg_accounts_hist ( account_num, valid_to, client, effective_from, effective_to, deleted_flg )
select 
	stg.account_num,
	stg.valid_to,
	stg.client,
	stg.create_dt,
	cast(coalesce(lead(stg.update_dt) over (partition by stg.account_num  order by stg.update_dt) - interval '1 second',
to_date('9999-12-31','YYYY-MM-DD')) as timestamp) effective_to,
	'N' 
from deaise.voal_stg_accounts stg
 	left join deaise.voal_stg_accounts_hist tgt
 	on stg.account_num = tgt.account_num 
where tgt.account_num is null;

-- 3. Обновление в приемнике "обновлений" на источнике (формат SCD2).
insert into deaise.voal_stg_accounts_hist ( account_num, valid_to, client, effective_from, effective_to, deleted_flg )
select 
	stg.account_num,
	stg.valid_to,
	stg.client,
	stg.create_dt,
	to_date('9999-12-31','YYYY-MM-DD') as timestamp ,
	'N' 
from deaise.voal_stg_accounts stg 
    inner join deaise.voal_stg_accounts_hist tgt
    on stg.account_num = tgt.account_num
    and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
where ( stg.valid_to <> tgt.valid_to or (stg.valid_to is null and tgt.valid_to is not null) or (stg.valid_to is not null and tgt.valid_to is null)
        or stg.client <> tgt.client or (stg.client is null and tgt.client is not null) or (stg.client is not null and tgt.client is null))
        or tgt.deleted_flg = 'Y';

update deaise.voal_stg_accounts_hist tgt 
   set effective_to = tmp.create_dt - interval '1 second'
from (
	select 
		stg.account_num,
		stg.valid_to,
		stg.client,
		stg.create_dt
	from deaise.voal_stg_accounts stg 
	inner join deaise.voal_stg_accounts_hist tgt
		on stg.account_num = tgt.account_num
	and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	where ( stg.valid_to <> tgt.valid_to or (stg.valid_to is null and tgt.valid_to is not null) or (stg.valid_to is not null and tgt.valid_to is null)
        or stg.client <> tgt.client or (stg.client is null and tgt.client is not null) or (stg.client is not null and tgt.client is null))
        or tgt.deleted_flg = 'Y') tmp
where tgt.account_num = tmp.account_num
  	and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
 	and ( tmp.valid_to <> tgt.valid_to or (tmp.valid_to is null and tgt.valid_to is not null) or (tmp.valid_to is not null and tgt.valid_to is null)
        or tmp.client <> tgt.client or (tmp.client is null and tgt.client is not null) or (tmp.client is not null and tgt.client is null))
        or tgt.deleted_flg = 'Y';

-- 4. Обработка удалений в приемнике (формат SCD2).
insert into deaise.voal_stg_accounts_hist ( account_num, valid_to, client, effective_from, effective_to, deleted_flg )
select 
    tgt.account_num,
	tgt.valid_to,
	tgt.client, 
	now() start_dt,
	to_date('9999-12-31','YYYY-MM-DD') end_dt,
	'Y' deleted_flg
from deaise.voal_stg_accounts_hist tgt 
left join deaise.voal_stg_accounts_del stg
  	on stg.account_num = tgt.account_num
where stg.account_num is null
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and tgt.deleted_flg = 'N';

update deaise.voal_stg_accounts_hist tgt 
   set effective_to = now() - interval '1 second'
where tgt.account_num in (
	select 
		tgt.account_num
	from deaise.voal_stg_accounts_hist tgt 
	left join deaise.voal_stg_accounts_del stg
		on stg.account_num = tgt.account_num
	where stg.account_num is null
	  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	  and deleted_flg = 'N')
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and deleted_flg = 'N';

-- 5. Обновление метаданных.

update deaise.voal_meta_stg
  set max_update_dt = coalesce((select max(update_dt) from deaise.voal_stg_accounts), now())
where schema_name = 'info' and table_name = 'accounts';