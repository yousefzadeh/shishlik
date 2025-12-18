with ixcol as (
    select
    object_id,
    index_id,
    index_column_id,
    column_id,
    key_ordinal,
    partition_ordinal,
    is_descending_key,
    is_included_column,
    column_store_order_ordinal
    from sys.index_columns
)
select * from ixcol