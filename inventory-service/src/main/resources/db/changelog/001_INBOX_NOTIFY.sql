/* inbox insertion channel notification */
create or replace function notify_inbox_channel() returns trigger
    language plpgsql
as
$$
BEGIN
    PERFORM pg_notify('inbox_channel', NEW.id);
    RETURN NEW;
END;
$$;

/* inbox insertion trigger */
create trigger inbox_insert_trigger
    after insert
    on orders_inbox
    for each row
execute procedure notify_inbox_channel();
