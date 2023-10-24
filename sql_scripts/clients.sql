delete from deaise.voal_stg_clients_del;

-- 1. Захват в стейджинг ключей из источника полным срезом для вычисления удалений.
insert into deaise.voal_stg_clients_del ( client_id )
select client_id from deaise.voal_stg_clients;

-- 2. Загрузка в приемник "вставок" на источнике (формат SCD2)
insert into deaise.voal_stg_clients_hist ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg )
select 
	stg.client_id,
	stg.last_name,
	stg.first_name,
	stg.patronymic,
	stg.date_of_birth,
	stg.passport_num,
	stg.passport_valid_to,
	stg.phone,
	stg.create_dt,
	cast(coalesce(lead(stg.update_dt) over (partition by stg.client_id  order by stg.update_dt) - interval '1 second',
to_date('9999-12-31','YYYY-MM-DD')) as timestamp) effective_to,
	'N' 
from deaise.voal_stg_clients stg
 	left join deaise.voal_stg_clients_hist tgt
 	on stg.client_id = tgt.client_id 
where tgt.client_id is null;

-- 3. Обновление в приемнике "обновлений" на источнике (формат SCD2).
insert into deaise.voal_stg_clients_hist ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg )
select 
	stg.client_id,
	stg.last_name,
	stg.first_name,
	stg.patronymic,
	stg.date_of_birth,
	stg.passport_num,
	stg.passport_valid_to,
	stg.phone,
	stg.create_dt,
	to_date('9999-12-31','YYYY-MM-DD') as timestamp ,
	'N' 
from deaise.voal_stg_clients stg 
    inner join deaise.voal_stg_clients_hist tgt
    on stg.client_id = tgt.client_id
    and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
where ( stg.last_name <> tgt.last_name or (stg.last_name is null and tgt.last_name is not null) or (stg.last_name is not null and tgt.last_name is null)
        or stg.first_name <> tgt.first_name or (stg.first_name is null and tgt.first_name is not null) or (stg.first_name is not null and tgt.first_name is null)
        or stg.patronymic <> tgt.patronymic or (stg.patronymic is null and tgt.patronymic is not null) or (stg.patronymic is not null and tgt.patronymic is null)
        or stg.date_of_birth <> tgt.date_of_birth or (stg.date_of_birth is null and tgt.date_of_birth is not null) or (stg.date_of_birth is not null and tgt.date_of_birth is null)
        or stg.passport_num <> tgt.passport_num or (stg.passport_num is null and tgt.passport_num is not null) or (stg.passport_num is not null and tgt.passport_num is null)
        or stg.passport_valid_to <> tgt.passport_valid_to or (stg.passport_valid_to is null and tgt.passport_valid_to is not null) or (stg.passport_valid_to is not null and tgt.passport_valid_to is null)
        or stg.phone <> tgt.phone or (stg.phone is null and tgt.phone is not null) or (stg.phone is not null and tgt.phone is null))
        or tgt.deleted_flg = 'Y';

update deaise.voal_stg_clients_hist tgt 
   set effective_to = tmp.create_dt - interval '1 second'
from (
	select 
		stg.client_id,
		stg.last_name,
		stg.first_name,
		stg.patronymic,
		stg.date_of_birth,
		stg.passport_num,
		stg.passport_valid_to,
		stg.phone,
		stg.create_dt
	from deaise.voal_stg_clients stg 
	inner join deaise.voal_stg_clients_hist tgt
	  	on stg.client_id = tgt.client_id
	 	and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	where ( stg.last_name <> tgt.last_name or (stg.last_name is null and tgt.last_name is not null) or (stg.last_name is not null and tgt.last_name is null)
        or stg.first_name <> tgt.first_name or (stg.first_name is null and tgt.first_name is not null) or (stg.first_name is not null and tgt.first_name is null)
        or stg.patronymic <> tgt.patronymic or (stg.patronymic is null and tgt.patronymic is not null) or (stg.patronymic is not null and tgt.patronymic is null)
        or stg.date_of_birth <> tgt.date_of_birth or (stg.date_of_birth is null and tgt.date_of_birth is not null) or (stg.date_of_birth is not null and tgt.date_of_birth is null)
        or stg.passport_num <> tgt.passport_num or (stg.passport_num is null and tgt.passport_num is not null) or (stg.passport_num is not null and tgt.passport_num is null)
        or stg.passport_valid_to <> tgt.passport_valid_to or (stg.passport_valid_to is null and tgt.passport_valid_to is not null) or (stg.passport_valid_to is not null and tgt.passport_valid_to is null)
        or stg.phone <> tgt.phone or (stg.phone is null and tgt.phone is not null) or (stg.phone is not null and tgt.phone is null))
        or tgt.deleted_flg = 'Y') tmp
where tgt.client_id = tmp.client_id
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and ( tmp.last_name <> tgt.last_name or (tmp.last_name is null and tgt.last_name is not null) or (tmp.last_name is not null and tgt.last_name is null)
        or tmp.first_name <> tgt.first_name or (tmp.first_name is null and tgt.first_name is not null) or (tmp.first_name is not null and tgt.first_name is null)
        or tmp.patronymic <> tgt.patronymic or (tmp.patronymic is null and tgt.patronymic is not null) or (tmp.patronymic is not null and tgt.patronymic is null)
        or tmp.date_of_birth <> tgt.date_of_birth or (tmp.date_of_birth is null and tgt.date_of_birth is not null) or (tmp.date_of_birth is not null and tgt.date_of_birth is null)
        or tmp.passport_num <> tgt.passport_num or (tmp.passport_num is null and tgt.passport_num is not null) or (tmp.passport_num is not null and tgt.passport_num is null)
        or tmp.passport_valid_to <> tgt.passport_valid_to or (tmp.passport_valid_to is null and tgt.passport_valid_to is not null) or (tmp.passport_valid_to is not null and tgt.passport_valid_to is null)
        or tmp.phone <> tgt.phone or (tmp.phone is null and tgt.phone is not null) or (tmp.phone is not null and tgt.phone is null))
        or tgt.deleted_flg = 'Y';

-- 4. Обработка удалений в приемнике (формат SCD2).
insert into deaise.voal_stg_clients_hist ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg )
select 
    tgt.client_id,
	tgt.last_name,
	tgt.first_name,
	tgt.patronymic,
	tgt.date_of_birth,
	tgt.passport_num,
	tgt.passport_valid_to,
	tgt.phone,
	now() start_dt,
	to_date('9999-12-31','YYYY-MM-DD') end_dt,
	'Y' deleted_flg
from deaise.voal_stg_clients_hist tgt 
left join deaise.voal_stg_clients_del stg
  	on stg.client_id = tgt.client_id
where stg.client_id is null
  	and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  	and tgt.deleted_flg = 'N';

update deaise.voal_stg_clients_hist tgt 
   set effective_to = now() - interval '1 second'
where tgt.client_id in (
	select 
		tgt.client_id
	from deaise.voal_stg_clients_hist tgt left join 
		 deaise.voal_stg_clients_del stg
	  on stg.client_id = tgt.client_id
	where stg.client_id is null
	  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
	  and deleted_flg = 'N')
  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and deleted_flg = 'N';

-- 5. Обновление метаданных.

update deaise.voal_meta_stg
  set max_update_dt = coalesce((select max(update_dt) from deaise.voal_stg_clients), now())
where schema_name = 'info' and table_name = 'clients';