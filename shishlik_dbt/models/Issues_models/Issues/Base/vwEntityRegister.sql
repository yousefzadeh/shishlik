{{ config(materialized="view") }}
with
    base as (
        select
        Id,
        Name,
        Description,
        EntityType,
        RegisterRecordLabel,
        RegisterRecordLabelPlural,
        IsWorkflowEnabled,
        MigratedFromRegisterId,
        TenantId,
        Uuid,
        CreationTime,
        CreatorUserId,
        LastModificationTime,
        LastModifierUserId,
        Color,
        Icon
        from {{ source("issue_models", "EntityRegister") }} 
        {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "EntityRegister") }},
    {{ col_rename("Name", "EntityRegister") }},
    {{ col_rename("Description", "EntityRegister") }},
    {{ col_rename("EntityType", "EntityRegister") }},
    {{ col_rename("RegisterRecordLabel", "EntityRegister") }},
    {{ col_rename("RegisterRecordLabelPlural", "EntityRegister") }},
    {{ col_rename("IsWorkflowEnabled", "EntityRegister") }},
    {{ col_rename("MigratedFromRegisterId", "EntityRegister") }},
    {{ col_rename("TenantId", "EntityRegister") }},
    {{ col_rename("Uuid", "EntityRegister") }},
    {{ col_rename("CreationTime", "EntityRegister") }},
    {{ col_rename("CreatorUserId", "EntityRegister") }},
    {{ col_rename("LastModificationTime", "EntityRegister") }},
    {{ col_rename("LastModifierUserId", "EntityRegister") }},
    {{ col_rename("Color", "EntityRegister") }},
    {{ col_rename("Icon", "EntityRegister") }}
from
    base