with
    base as (select * from {{ ref("vwPolicyStakeHolders") }}),
    user_row as (
        select
            PolicyStakeHolders_TenantId Tenant_Id,
            PolicyStakeHolders_PolicyId ControlSet_Id,
            case
                PolicyStakeHolders_RoleCode when 'Owner' then u.AbpUsers_FullName
            end OwnerName,
            case
                PolicyStakeHolders_RoleCode when 'Reviewers' then u.AbpUsers_FullName
            end ReviewerName,
            case
                PolicyStakeHolders_RoleCode when 'Readers' then u.AbpUsers_FullName
            end ReaderName,
            case
                PolicyStakeHolders_RoleCode when 'Approvers' then u.AbpUsers_FullName
            end ApproverName
        from base
        join {{ ref("vwAbpUser") }} u on base.PolicyStakeHolders_UserId = u.AbpUsers_Id
    ),
    org_row as (
        select
            PolicyStakeHolders_TenantId Tenant_Id,
            PolicyStakeHolders_PolicyId ControlSet_Id,
            case
                PolicyStakeHolders_RoleCode
                when 'Owner'
                then o.AbpOrganizationUnits_DisplayName
            end OwnerName,
            case
                PolicyStakeHolders_RoleCode
                when 'Reviewers'
                then o.AbpOrganizationUnits_DisplayName
            end ReviewerName,
            case
                PolicyStakeHolders_RoleCode
                when 'Readers'
                then o.AbpOrganizationUnits_DisplayName
            end ReaderName,
            case
                PolicyStakeHolders_RoleCode
                when 'Approvers'
                then o.AbpOrganizationUnits_DisplayName
            end ApproverName
        from base
        join
            {{ ref("vwAbpOrganizationUnits") }} o
            on base.PolicyStakeHolders_OrganizationUnitId = o.AbpOrganizationUnits_Id
    ),
    final_row as (
        select *
        from user_row
        union all
        select *
        from org_row
    ),
    final as (
        select
            Tenant_Id,
            ControlSet_Id,
            string_agg(OwnerName, ', ') OwnerList,
            string_agg(ReviewerName, ', ') ReviewerList,
            string_agg(ReaderName, ', ') ReaderList,
            string_agg(ApproverName, ', ') ApproverList
        from final_row
        group by Tenant_Id, ControlSet_Id
    )
select *
from final
