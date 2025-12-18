{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [RiskId], [UserId], [OrganizationUnitId]
        from {{ source("risk_models", "RiskOwner") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskOwner") }},
    {{ col_rename("CreationTime", "RiskOwner") }},
    {{ col_rename("CreatorUserId", "RiskOwner") }},
    {{ col_rename("LastModificationTime", "RiskOwner") }},

    {{ col_rename("LastModifierUserId", "RiskOwner") }},
    {{ col_rename("IsDeleted", "RiskOwner") }},
    {{ col_rename("DeleterUserId", "RiskOwner") }},
    {{ col_rename("DeletionTime", "RiskOwner") }},

    {{ col_rename("TenantId", "RiskOwner") }},
    {{ col_rename("RiskId", "RiskOwner") }},
    {{ col_rename("UserId", "RiskOwner") }},
    {{ col_rename("OrganizationUnitId", "RiskOwner") }},
    u.AbpUsers_FullName RiskOwner_FullName,
    u.AbpUsers_UserName RiskOwner_UserName,
    o.AbpOrganizationUnits_DisplayName RiskOwner_OrganisationName
from base
left join {{ ref("vwAbpUser") }} u on base.UserId = u.AbpUsers_Id
left join {{ ref("vwAbpOrganizationUnits") }} o on base.OrganizationUnitId = o.AbpOrganizationUnits_Id
