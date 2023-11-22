delete from deaise.voal_stg_cards_del;

-- 1. Захват в стейджинг ключей из источника полным срезом для вычисления удалений.
insert into deaise.voal_stg_cards_del ( card_num )
select card_num from deaise.voal_stg_cards;

-- 2. Загрузка в приемник "вставок" на источнике (формат SCD2)
insert into deaise.voal_dwh_dim_cards_hist ( card_num, account_num, effective_from, effective_to, deleted_flg )
select 
	stg.card_num, 
	stg.account_num,
	stg.create_dt,
	cast(coalesce(lead(stg.update_dt) over (partition by stg.account_num  order by stg.update_dt) - interval '1 second',
        to_date('9999-12-31','YYYY-MM-DD')) as timestamp) effective_to,
	'N' 
from deaise.voal_stg_cards stg
 	left join deaise.voal_dwh_dim_cards_hist tgt
 	on stg.card_num = tgt.card_num 
where tgt.card_num is null;

-- 3. Обновление в приемнике "обновлений" на источнике (формат SCD2).
insert into deaise.voal_dwh_dim_cards_hist ( card_num, account_num, effective_from, effective_to, deleted_flg )
select 
	stg.card_num, 
	stg.account_num,
	stg.create_dt,
	to_date('9999-12-31','YYYY-MM-DD') as timestamp ,
	'N' 
from deaise.voal_stg_cards stg 
    inner join deaise.voal_dwh_dim_cards_hist tgt
    on stg.card_num = tgt.card_num
    and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
where ( stg.card_num <> tgt.card_num 
        or (stg.card_num is null and tgt.card_num is not null) 
        or (stg.card_num is not null and tgt.card_num is null))
        or tgt.deleted_flg = 'Y';

update deaise.voal_dwh_dim_cards_hist tgt 
   set effective_to = tmp.create_dt - interval '1 second'
from (
	select 
		stg.card_num, 
		stg.account_num,
		stg.create_dt
	from deaise.voal_stg_cards stg inner join 
		 deaise.voal_dwh_dim_cards_hist tgt
	  on stg.card_num = tgt.card_num
	 and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	where ( stg.account_num <> tgt.account_num 
        or (stg.account_num is null and tgt.account_num is not null) 
        or (stg.account_num is not null and tgt.account_num is null))
        or tgt.deleted_flg = 'Y') tmp
where tgt.card_num = tmp.card_num
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and (  tmp.account_num <> tgt.account_num 
        or (tmp.account_num is null and tgt.account_num is not null) 
        or (tmp.account_num is not null and tgt.account_num is null))
        or tgt.deleted_flg = 'Y';

-- 4. Обработка удалений в приемнике (формат SCD2).
insert into deaise.voal_dwh_dim_cards_hist ( card_num, account_num, effective_from, effective_to, deleted_flg )
select 
    tgt.card_num, 
	tgt.account_num,
	now() start_dt,
	to_date('9999-12-31','YYYY-MM-DD') end_dt,
	'Y' deleted_flg
from deaise.voal_dwh_dim_cards_hist tgt 
left join deaise.voal_stg_cards_del stg
  on stg.card_num = tgt.card_num
where stg.card_num is null
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and tgt.deleted_flg = 'N';

update deaise.voal_dwh_dim_cards_hist tgt 
   set effective_to = now() - interval '1 second'
where tgt.card_num in (
	select 
		tgt.card_num
	from deaise.voal_dwh_dim_cards_hist tgt 
	left join deaise.voal_stg_cards_del stg
	  	on stg.card_num = tgt.card_num
	where stg.card_num is null
	  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	  and deleted_flg = 'N')
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and deleted_flg = 'N';

-- 5. Обновление метаданных.

update deaise.voal_meta_stg
  set max_update_dt = coalesce((select max(update_dt) from deaise.voal_stg_cards), now())
where schema_name = 'info' and table_name = 'cards';