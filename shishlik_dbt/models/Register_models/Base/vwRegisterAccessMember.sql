{{ config(materialized="view") }}

with
    base as (
        select
            Id,
            RegisterId,
            UserId,
            OrganizationUnitId,
            TenantId,
            CreationTime,
            CreatorUserId,
            LastModificationTime,
            LastModifierUserId,
            DeleterUserId,
            DeletionTime

        from {{ source("register_models", "RegisterAccessMember") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RegisterAccessMember") }},
    {{ col_rename("RegisterId", "RegisterAccessMember") }},
    {{ col_rename("UserId", "RegisterAccessMember") }},
    {{ col_rename("OrganizationUnitId", "RegisterAccessMember") }},
    {{ col_rename("TenantId", "RegisterAccessMember") }},

    {{ col_rename("CreationTime", "RegisterAccessMember") }},
    {{ col_rename("CreatorUserId", "RegisterAccessMember") }},
    {{ col_rename("LastModificationTime", "RegisterAccessMember") }},
    {{ col_rename("LastModifierUserId", "RegisterAccessMember") }},

    {{ col_rename("DeleterUserId", "RegisterAccessMember") }},
    {{ col_rename("DeletionTime", "RegisterAccessMember") }},
    au.AbpUsers_FullName RegisterAccessMember_User,
    aou.AbpOrganizationUnits_DisplayName RegisterAccessMember_Organization
from base
left join {{ ref("vwAbpUser") }} au on au.AbpUsers_Id = base.UserId
left join {{ ref("vwAbpOrganizationUnits") }} aou on aou.AbpOrganizationUnits_Id = base.OrganizationUnitId
