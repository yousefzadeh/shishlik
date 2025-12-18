{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [RiskId], [UserId], [OrganizationUnitId], [TenantId]
        from {{ source("risk_models", "RiskUser") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskUser") }},
    {{ col_rename("CreationTime", "RiskUser") }},
    {{ col_rename("CreatorUserId", "RiskUser") }},
    {{ col_rename("LastModificationTime", "RiskUser") }},

    {{ col_rename("LastModifierUserId", "RiskUser") }},
    {{ col_rename("IsDeleted", "RiskUser") }},
    {{ col_rename("DeleterUserId", "RiskUser") }},
    {{ col_rename("DeletionTime", "RiskUser") }},

    {{ col_rename("RiskId", "RiskUser") }},
    {{ col_rename("UserId", "RiskUser") }},
    {{ col_rename("OrganizationUnitId", "RiskUser") }},
    {{ col_rename("TenantId", "RiskUser") }},
    u.AbpUsers_FullName RiskUser_FullName,
    u.AbpUsers_UserName RiskUser_UserName,
    o.AbpOrganizationUnits_DisplayName RiskUser_OrganisationName
from base
left join {{ ref("vwAbpUser") }} u on base.UserId = u.AbpUsers_Id
left join {{ ref("vwAbpOrganizationUnits") }} o on base.OrganizationUnitId = o.AbpOrganizationUnits_Id
