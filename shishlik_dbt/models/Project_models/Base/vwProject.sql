{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([Name] as nvarchar(4000))[Name],
            cast([Description] as nvarchar(4000)) Description,
            [TenantId],
            [OwnerId],
            [Status],
            [TemplateStatus],
            [TemplateType],
            [IsTemplate],
            [DueDate],
            [PublishedDate],
            [PublishedById],
            [CreatedFromId]
        from {{ source("project_models", "Project") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "Project") }},
    {{ col_rename("CreationTime", "Project") }},
    {{ col_rename("LastModificationTime", "Project") }},
    {{ col_rename("Name", "Project") }},
    {{ col_rename("Description", "Project") }},
    {{ col_rename("TenantId", "Project") }},

    {{ col_rename("OwnerId", "Project") }},
    {{ col_rename("Status", "Project") }},
    {{ col_rename("TemplateStatus", "Project") }},
    {{ col_rename("TemplateType", "Project") }},

    {{ col_rename("IsTemplate", "Project") }},
    {{ col_rename("DueDate", "Project") }},
    {{ col_rename("PublishedDate", "Project") }},
    {{ col_rename("PublishedById", "Project") }},

    {{ col_rename("CreatedFromId", "Project") }}
from base
