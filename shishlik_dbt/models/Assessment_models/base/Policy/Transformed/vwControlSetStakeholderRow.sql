with
    base as (select * from {{ ref("vwPolicyStakeHolders") }}),
    user_row as (
        select
            PolicyStakeHolders_TenantId Tenant_Id,
            PolicyStakeHolders_PolicyId ControlSet_Id,
            PolicyStakeHolders_RoleCode,
            u.AbpUsers_FullName PolicyStakeHolders_Name
        from base
        join {{ ref("vwAbpUser") }} u on base.PolicyStakeHolders_UserId = u.AbpUsers_Id
    ),
    org_row as (
        select
            PolicyStakeHolders_TenantId Tenant_Id,
            PolicyStakeHolders_PolicyId ControlSet_Id,
            PolicyStakeHolders_RoleCode,
            o.AbpOrganizationUnits_DisplayName PolicyStakeHolders_Name
        from base
        join
            {{ ref("vwAbpOrganizationUnits") }} o
            on base.PolicyStakeHolders_OrganizationUnitId = o.AbpOrganizationUnits_Id
    ),
    final as (
        select *
        from user_row
        union all
        select *
        from org_row
    )
select *
from final
