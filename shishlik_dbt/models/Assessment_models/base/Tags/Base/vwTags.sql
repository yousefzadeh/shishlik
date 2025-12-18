{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([Name] as nvarchar(4000))[Name],
            cast([Description] as nvarchar(4000)) Description,
            [Type],
            [TenantId],
            coalesce([LastModificationTime],[CreationTime]) as [UpdateTime]
        from {{ source("assessment_models", "Tags") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "Tags") }},
    {{ col_rename("Name", "Tags") }},
    {{ col_rename("Description", "Tags") }},
    {{ col_rename("Type", "Tags") }},
    {{ col_rename("TenantId", "Tags") }},
    {{ col_rename("UpdateTime", "Tags") }}
from base
