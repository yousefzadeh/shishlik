{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [CustomFieldId],
            cast([AttributeName] as nvarchar(4000)) AttributeName
        from {{ source("assessment_models", "CustomFieldAttribute") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "CustomFieldAttribute") }},
    {{ col_rename("TenantId", "CustomFieldAttribute") }},
    {{ col_rename("CustomFieldId", "CustomFieldAttribute") }},
    {{ col_rename("AttributeName", "CustomFieldAttribute") }}
from base
