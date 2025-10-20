create or replace function fn_users_audit()
returns trigger
as $$
  begin
	if new.name != old.name then
		insert into users_audit (user_id,changed_by,field_changed,old_value,new_value)
		values(old.id, user, 'name', old.name, new.name);
	end if;
	if new.email != old.email then 
		insert into users_audit (user_id,changed_by,field_changed,old_value,new_value)
	values(old.id, user, 'email', old.email, new.email);
	end if;
	if new.role != old.role then 
		insert into users_audit (user_id,changed_by,field_changed,old_value,new_value)
		values(old.id, user, 'role', old.role, new.role);
	end if;
  return new;
  end;
$$
language plpgsql


CREATE TRIGGER trigger_log_user_update
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION fn_users_audit();


CREATE EXTENSION IF NOT exists pg_cron;


create or replace function fn_upload_today_changes()
returns void
as $$
  declare 
	path_ text;
  begin
	path_ := '/tmp/users_audit_export_' || now()::date::text || '.csv';
	execute format('copy ( select * from users_audit a where a.changed_at::date = now()::date ) 
	to %L
	delimiter '','' csv header', path_);
  end;
$$
language plpgsql


select cron.schedule('data_export','0 3 * * *', $$ select fn_upload_today_changes(); $$);


