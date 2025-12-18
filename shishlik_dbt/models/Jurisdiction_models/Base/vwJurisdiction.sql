{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, cast([Name] as nvarchar(4000))[Name]
        from {{ source("jurisdiction_models", "Jurisdiction") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Jurisdiction") }},
    {{ col_rename("CreationTime", "Jurisdiction") }},
    {{ col_rename("CreatorUserId", "Jurisdiction") }},
    {{ col_rename("Name", "Jurisdiction") }}
from base
