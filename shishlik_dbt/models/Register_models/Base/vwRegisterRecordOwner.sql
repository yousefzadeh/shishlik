{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [RegisterRecordId], [UserId], [OrganizationUnitId]
        from {{ source("register_models", "RegisterRecordOwner") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RegisterRecordOwner") }},
    {{ col_rename("CreationTime", "RegisterRecordOwner") }},
    {{ col_rename("LastModificationTime", "RegisterRecordOwner") }},
    {{ col_rename("TenantId", "RegisterRecordOwner") }},
    {{ col_rename("RegisterRecordId", "RegisterRecordOwner") }},
    {{ col_rename("UserId", "RegisterRecordOwner") }},
    {{ col_rename("OrganizationUnitId", "RegisterRecordOwner") }},
    au.AbpUsers_FullName RegisterRecordOwner_User,
    aou.AbpOrganizationUnits_DisplayName RegisterRecordOwner_Organization
from base
left join {{ ref("vwAbpUser") }} au on au.AbpUsers_Id = base.UserId
left join {{ ref("vwAbpOrganizationUnits") }} aou on aou.AbpOrganizationUnits_Id = base.OrganizationUnitId
