/* https://dev.to/mcadariu/postgres-column-tetris-neatly-packing-your-tables-for-fun-and-profit-1j6g */

create table orders_inbox
(
    "createdAt"   timestamp(6) with time zone not null,
    "processedAt" timestamp(6) with time zone,
    "updatedAt"   timestamp(6) with time zone not null,
    version       integer default 0           not null,
    status        smallint,
    id            varchar(13)                 not null,
    "orderId"     varchar(13)                 not null,
    "eventType"   varchar(100)                not null,
    payload       jsonb                       not null,
    primary key (id)
);

/* unprocessed inbox rows index */
create index idx_orders_inbox_unprocessed on orders_inbox ("createdAt") where "processedAt" is null;
