{{ config(materialized="view") }}
with
    base as (
        select
        Id,
        Name RegisterName,
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
        Icon,
        Coalesce(LastModificationTime,CreationTime) UpdateTime
        from {{ source("issue_models", "EntityRegister") }} 
        {{ system_remove_IsDeleted() }}
        and EntityType = 4
    )

select
    {{ col_rename("Id", "Register") }},
    {{ col_rename("RegisterName", "Register") }},
    {{ col_rename("Description", "Register") }},
    {{ col_rename("EntityType", "Register") }},
    {{ col_rename("RegisterRecordLabel", "Register") }},
    {{ col_rename("RegisterRecordLabelPlural", "Register") }},
    {{ col_rename("IsWorkflowEnabled", "Register") }},
    {{ col_rename("MigratedFromRegisterId", "Register") }},
    {{ col_rename("TenantId", "Register") }},
    {{ col_rename("Uuid", "Register") }},
    {{ col_rename("CreationTime", "Register") }},
    {{ col_rename("CreatorUserId", "Register") }},
    {{ col_rename("LastModificationTime", "Register") }},
    {{ col_rename("LastModifierUserId", "Register") }},
    {{ col_rename("Color", "Register") }},
    {{ col_rename("Icon", "Register") }},
    {{ col_rename("UpdateTime", "Register") }}
from
    base