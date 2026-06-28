/* outbox insertion channel notification */
create or replace function notify_outbox_insert() returns trigger
    language plpgsql
as
$$
BEGIN
    PERFORM pg_notify('outbox_insert_channel', NEW.id);
    RETURN NEW;
END;
$$;

/* outbox insertion trigger */
create trigger outbox_insert_trigger
    after insert
    on orders_outbox
    for each row
execute procedure notify_outbox_insert();
