{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [StatementId] ResponsibilityId, [UserId], [OrganizationUnitId]
        from {{ source("statement_models", "StatementMember") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ResponsibilityAssignee") }},
    {{ col_rename("TenantId", "ResponsibilityAssignee") }},
    {{ col_rename("ResponsibilityId", "ResponsibilityAssignee") }},
    {{ col_rename("UserId", "ResponsibilityAssignee") }},

    {{ col_rename("OrganizationUnitId", "ResponsibilityAssignee") }},
    u.AbpUsers_FullName RiskOwner_FullName,
    o.AbpOrganizationUnits_DisplayName RiskOwner_OrganisationName
from base
left join {{ ref("vwAbpUser") }} u on base.UserId = u.AbpUsers_Id
left join {{ ref("vwAbpOrganizationUnits") }} o on base.OrganizationUnitId = o.AbpOrganizationUnits_Id
