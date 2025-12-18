{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [IssueId], [UserId], [OrganizationUnitId], [TenantId]
        from {{ source("issue_models", "IssueUser") }} {{ system_remove_IsDeleted() }}
    )

select distinct
    {{ col_rename("Id", "IssueUser") }},
    {{ col_rename("IssueId", "IssueUser") }},
    {{ col_rename("CreationTime", "IssueUser") }},
    {{ col_rename("LastModificationTime", "IssueUser") }},
    {{ col_rename("UserId", "IssueUser") }},
    {{ col_rename("OrganizationUnitId", "IssueUser") }},

    {{ col_rename("TenantId", "IssueUser") }},
    u.AbpUsers_FullName IssueUser_FullName,
    u.AbpUsers_UserName IssueUser_UserName,
    o.AbpOrganizationUnits_DisplayName IssueUser_OrganisationName
from base
left join {{ ref("vwAbpUser") }} u on base.UserId = u.AbpUsers_Id
left join {{ ref("vwAbpOrganizationUnits") }} o on base.OrganizationUnitId = o.AbpOrganizationUnits_Id
