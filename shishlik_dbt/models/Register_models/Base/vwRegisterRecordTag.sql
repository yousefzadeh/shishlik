{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [RegisterRecordId], [TagId]
        from {{ source("register_models", "RegisterRecordTag") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RegisterRecordTag") }},
    {{ col_rename("TenantId", "RegisterRecordTag") }},
    {{ col_rename("RegisterRecordId", "RegisterRecordTag") }},
    {{ col_rename("TagId", "RegisterRecordTag") }}
from base
