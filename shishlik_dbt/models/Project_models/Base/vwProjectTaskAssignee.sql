{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [ProjectTaskId], [UserId], [OrganizationUnitId]
        from {{ source("project_models", "ProjectTaskAssignee") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ProjectTaskAssignee") }},
    {{ col_rename("CreationTime", "ProjectTaskAssignee") }},
    {{ col_rename("LastModificationTime", "ProjectTaskAssignee") }},
    {{ col_rename("TenantId", "ProjectTaskAssignee") }},
    {{ col_rename("ProjectTaskId", "ProjectTaskAssignee") }},
    {{ col_rename("UserId", "ProjectTaskAssignee") }},

    {{ col_rename("OrganizationUnitId", "ProjectTaskAssignee") }}
from base
