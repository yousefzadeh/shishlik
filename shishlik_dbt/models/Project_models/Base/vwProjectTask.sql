{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([Name] as nvarchar(4000))[Name],
            cast([Description] as nvarchar(4000)) Description,
            [ParentTaskId],
            [ProjectId],
            [TenantId],
            [Ordinal],
            [DueDate],
            [Status],
            [HasComments],
            [HasDocuments]
        from {{ source("project_models", "ProjectTask") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ProjectTask") }},
    {{ col_rename("Name", "ProjectTask") }},
    {{ col_rename("Description", "ProjectTask") }},
    {{ col_rename("ParentTaskId", "ProjectTask") }},

    {{ col_rename("ProjectId", "ProjectTask") }},
    {{ col_rename("TenantId", "ProjectTask") }},
    {{ col_rename("Ordinal", "ProjectTask") }},
    {{ col_rename("DueDate", "ProjectTask") }},

    {{ col_rename("Status", "ProjectTask") }},
    {{ col_rename("HasComments", "ProjectTask") }},
    {{ col_rename("HasDocuments", "ProjectTask") }}
from base
