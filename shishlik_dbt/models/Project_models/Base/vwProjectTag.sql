{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TagId], [ProjectId], [TenantId]
        from {{ source("project_models", "ProjectTag") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ProjectTag") }},
    {{ col_rename("TagId", "ProjectTag") }},
    {{ col_rename("ProjectId", "ProjectTag") }},
    {{ col_rename("TenantId", "ProjectTag") }}
from base
