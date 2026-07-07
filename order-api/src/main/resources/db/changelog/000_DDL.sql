/* https://dev.to/mcadariu/postgres-column-tetris-neatly-packing-your-tables-for-fun-and-profit-1j6g */

create table orders
(
    "createdAt"  timestamp(6) with time zone not null,
    "updatedAt"  timestamp(6) with time zone not null,
    version      integer default 0           not null,
    id           varchar(13)                 not null,
    "customerId" varchar(13)                 not null,
    items        jsonb                       not null,
    primary key (id),
    constraint orders_version_check check (version >= 0)
);

create table orders_outbox
(
    "createdAt"   timestamp(6) with time zone not null,
    "publishedAt" timestamp(6) with time zone,
    "updatedAt"   timestamp(6) with time zone not null,
    attempts      integer default 0           not null,
    version       integer default 0           not null,
    id            varchar(13)                 not null,
    "orderId"     varchar(13)                 not null,
    "eventType"   varchar(100)                not null,
    topic         varchar(100)                not null,
    payload       jsonb                       not null,
    "lastError"   text,
    primary key (id),
    constraint orders_outbox_attempts_check check (attempts >= 0),
    constraint orders_outbox_version_check check (version >= 0)
);

/* unpublished outbox rows index */
create index idx_orders_outbox_unpublished on orders_outbox ("createdAt") where "publishedAt" is null;

/* dead letter outbox rows index */
create index idx_orders_outbox_dead_letter on orders_outbox ("createdAt") where "lastError" is not null;

/* dead letter outbox rows view */
create view dead_letters as
select id,
       "orderId",
       "eventType",
       topic,
       payload,
       attempts,
       "lastError",
       "createdAt",
       "updatedAt" as "lastErrorAt"
from orders_outbox
where "lastError" is not null;
