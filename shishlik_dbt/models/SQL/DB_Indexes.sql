with ix as (
    select
    object_id,
    name,
    index_id,
    [type],
    type_desc,
    is_unique,
    data_space_id,
    ignore_dup_key,
    is_primary_key,
    is_unique_constraint,
    fill_factor,
    is_padded,
    is_disabled,
    is_hypothetical,
    is_ignored_in_optimization,
    allow_row_locks,
    allow_page_locks,
    has_filter,
    filter_definition,
    compression_delay,
    suppress_dup_key_messages,
    auto_created,
    optimize_for_sequential_key
    from sys.indexes
)
select * from ix 