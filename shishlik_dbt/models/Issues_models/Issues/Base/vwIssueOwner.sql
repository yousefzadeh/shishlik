{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [IssueId], [UserId], [OrganizationUnitId]
        from {{ source("issue_models", "IssueOwner") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "IssueOwner") }},
    {{ col_rename("TenantId", "IssueOwner") }},
    {{ col_rename("IssueId", "IssueOwner") }},
    {{ col_rename("UserId", "IssueOwner") }},
    {{ col_rename("CreationTime", "IssueOwner") }},
    {{ col_rename("LastModificationTime", "IssueOwner") }},

    {{ col_rename("OrganizationUnitId", "IssueOwner") }},
    u.AbpUsers_FullName IssueOwner_FullName,
    u.AbpUsers_UserName IssueOwner_UserName,
    o.AbpOrganizationUnits_DisplayName IssueOwner_OrganisationName
from base
left join {{ ref("vwAbpUser") }} u on base.UserId = u.AbpUsers_Id
left join {{ ref("vwAbpOrganizationUnits") }} o on base.OrganizationUnitId = o.AbpOrganizationUnits_Id
