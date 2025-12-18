with
    {# Tenant Authority #}
    tenant_auth as (
        -- Tenant authority from Authority and TenantAuthority
        select distinct *
        from
            (
                select
                    'TenantAuthority'[source],
                    TenantAuthority_AuthorityId Authority_Id,
                    a.Authority_Name,
                    ta.TenantAuthority_TenantId Tenant_Id
                from {{ ref("vwTenantAuthority") }} ta
                join {{ ref("vwAuthority") }} a on a.Authority_Id = ta.TenantAuthority_AuthorityId

                union all

                select 'Authority'[source], Authority_Id, Authority_Name, Authority_TenantId
                from {{ ref("vwAuthority") }} a
            ) as T
    ),
    {# Authority Provision #}
    auth_prov_all_cols as (
        -- Authority - Provision
        -- 284,360 rows
        select *
        from tenant_auth a
        join {{ ref("vwAuthorityProvision") }} ap on ap.AuthorityProvision_AuthorityId = a.Authority_Id
    ),
    auth_prov as (
        -- 284,360 all rows
        -- 44,637 rows constrained on auth:source = Authority
        select Tenant_Id, [source], Authority_Id, Authority_Name, AuthorityProvision_Id, AuthorityProvision_Name
        from auth_prov_all_cols
    ),
    {# Controls : ControlSet - ControlDomain - Control #}
    ctrlset_detail_all_cols as (
        -- Controlset - domain - controls
        -- 36,909 rows of controls across all domains for all controlset
        select *
        from {{ ref("vwPolicy") }} p  -- dim table
        join
            {{ ref("vwPolicyDomain") }} pd  -- rel *:* to control
            on pd.PolicyDomain_PolicyId = p.Policy_Id
        join {{ ref("vwControls") }} c on c.Controls_PolicyDomainId = pd.PolicyDomain_Id
    ),
    {# Controls -> Provision -> Authority #}
    ctrlset_prov_auth as (
        select
            c.*,
            cp.ProvisionControl_Id,
            p.Tenant_Id,
            p. [source],
            p.Authority_Id,
            p.Authority_Name,
            p.AuthorityProvision_Id,
            p.AuthorityProvision_Name
        from ctrlset_detail_all_cols as c
        left join {{ ref("vwProvisionControl") }} cp on cp.ProvisionControl_ControlsId = c.Controls_Id
        left join auth_prov as p on p.AuthorityProvision_Id = cp.ProvisionControl_AuthorityReferenceId
    ),
    {# Responsibility Control #}
    respo_to_ctrl as (
        -- all statements with/without a relation to control
        select *
        from {{ ref("vwStatement") }} s
        left join {{ ref("vwStatementControl") }} sc on sc.StatementControl_StatementId = s.Statement_Id
    ),
    {# Control Responsibility #}
    ctrlset_respo as (
        -- all controls and any Responsibilities
        select *
        from ctrlset_prov_auth c
        left join respo_to_ctrl on respo_to_ctrl.StatementControl_ControlId = c.Controls_Id
    ),
    {# Responsibility Owner Assignee #}
    respo_owner_assignee as (
        select
            s.Statement_Id Responsibility_Id,
            so.StatementOwner_Id ResponsibilityOwner_Id,
            case
                when so.StatementOwner_UserId is null then 'Owner Organisation' else 'Owner User'
            end Responsibility_OwnerType,
            coalesce(so.StatementOwner_UserId, so.StatementOwner_OrganizationUnitId) Responsibility_OwnerId,
            sm.StatementMember_Id ResponsibilityAssignee_Id,
            case
                when sm.StatementMember_UserId is null then 'Assignee Organisation' else 'Assignee User'
            end Responsibility_AssigneeType,
            coalesce(sm.StatementMember_UserId, sm.StatementMember_OrganizationUnitId) Responsibility_AssigneeId
        from {{ ref("vwStatement") }} s
        join {{ ref("vwStatementOwner") }} so on so.StatementOwner_StatementId = s.Statement_Id
        join {{ ref("vwStatementMember") }} sm on sm.StatementMember_StatementId = s.Statement_Id
    ),
    respo_owner_name as (

        select
            r.Responsibility_Id,
            r.ResponsibilityOwner_Id,
            r.Responsibility_OwnerType,
            r.Responsibility_OwnerId,
            COALESCE(ou.AbpUsers_Name, 'User not found') Responsibility_OwnerName
        from respo_owner_assignee r
        left join
            {{ ref("vwAbpUser") }} ou
            on r.Responsibility_OwnerType = 'Owner User'
            and r.Responsibility_OwnerId = ou.AbpUsers_Id
        where r.Responsibility_OwnerType = 'Owner User'

        union all

        select
            r.Responsibility_Id,
            r.ResponsibilityOwner_Id,
            r.Responsibility_OwnerType,
            r.Responsibility_OwnerId,
            coalesce(oo.AbpOrganizationUnits_DisplayName, 'Organisation not found') OwnerName
        from respo_owner_assignee r
        left join
            {{ ref("vwAbpOrganizationUnits") }} oo
            on r.Responsibility_OwnerType = 'Owner Organisation'
            and r.Responsibility_OwnerId = oo.AbpOrganizationUnits_Id
        where r.Responsibility_OwnerId is not NULL and r.Responsibility_OwnerType = 'Owner Organisation'

        union all

        select
            r.Responsibility_Id,
            r.ResponsibilityOwner_Id,
            r.Responsibility_OwnerType,
            r.Responsibility_OwnerId,
            'No Owner' OwnerName
        from respo_owner_assignee r
        where r.Responsibility_OwnerId is NULL

    ),
    respo_assignee_name as (

        select
            r.Responsibility_Id,
            r.ResponsibilityAssignee_Id,
            r.Responsibility_AssigneeType,
            r.Responsibility_AssigneeId,
            COALESCE(ou.AbpUsers_Name, 'User not found') Responsibility_AssigneeName
        from respo_owner_assignee r
        left join
            {{ ref("vwAbpUser") }} ou
            on r.Responsibility_AssigneeType = 'Assignee User'
            and r.Responsibility_AssigneeId = ou.AbpUsers_Id
        where r.Responsibility_AssigneeType = 'Assignee User'

        union all

        select
            r.Responsibility_Id,
            r.ResponsibilityAssignee_Id,
            r.Responsibility_AssigneeType,
            r.Responsibility_AssigneeId,
            coalesce(oo.AbpOrganizationUnits_DisplayName, 'Organisation not found') AssigneeName
        from respo_owner_assignee r
        left join
            {{ ref("vwAbpOrganizationUnits") }} oo
            on r.Responsibility_AssigneeType = 'Assignee Organisation'
            and r.Responsibility_AssigneeId = oo.AbpOrganizationUnits_Id
        where r.Responsibility_AssigneeId is not NULL and r.Responsibility_AssigneeType = 'Owner Organisation'

        union all

        select
            r.Responsibility_Id,
            r.ResponsibilityAssignee_Id,
            r.Responsibility_AssigneeType,
            r.Responsibility_AssigneeId,
            'No Assignee' AssigneeName
        from respo_owner_assignee r
        where r.Responsibility_AssigneeId is NULL

    ),
    {# Counts #}
    {# Count of All Responsibilities #}
    respo_all as (
        select
            coalesce(Statement_RootStatementId, Statement_Id) Responsibility_RootId,
            Statement_Version Responsibility_Version,
            Statement_Id Responsibility_Id,  -- 12682
            Statement_IsCurrent Responsibility_IsCurrent,
            case when Statement_Status = 1 then 1 else 0 end Responsibility_IsEditing,
            case when Statement_Status = 2 then 1 else 0 end Responsibility_IsPublished,
            case when Statement_Status = 100 then 1 else 0 end Responsibility_IsDeprecated,
            cast(Statement_HasPeriod as int) Responsibility_HasPeriod
        from {{ ref("vwStatement") }}
    ),
    {# Unique List of Responsibilities with controls #}
    respo_ctrl as (
        select distinct Statement_Id Responsibility_Id  -- 11337
        from ctrlset_respo
    ),
    {# Unique List of Responsibilities with tasks #}
    respo_task as (
        select distinct Statement_Id Responsibility_Id  -- 422 statements
        from ctrlset_respo cr  -- Control Responsibility
        join
            {{ ref("vwStatementResponse") }} t  -- Task
            on cr.Statement_Id = t.StatementResponse_StatementId
    ),
    {# Unique List of Responsibilities with Owner #}
    respo_owner as (
        select distinct Responsibility_Id  -- 323 Responsibilities with owner
        from respo_owner_name
    ),
    {# Unique List of Responsibilities with Assignee #}
    respo_assignee as (
        select distinct Responsibility_Id  -- 282 Responsibilities
        from respo_assignee_name
    ),
    {# Final Results #}
    responsibility_flag as (

        select
            a.*,
            case when c.Responsibility_Id is NULL then 0 else 1 end HasControl,
            case when t.Responsibility_Id is NULL then 0 else 1 end HasAssignedTask,
            case when o.Responsibility_Id is NULL then 0 else 1 end HasOwner,
            case when ass.Responsibility_Id is NULL then 0 else 1 end HasAssignee
        from respo_all a
        left join respo_ctrl c on a.Responsibility_id = c.Responsibility_Id
        left join respo_task t on a.Responsibility_id = t.Responsibility_Id
        left join respo_owner o on a.Responsibility_id = o.Responsibility_Id
        left join respo_assignee ass on a.Responsibility_id = ass.Responsibility_Id
    )

select *
from responsibility_flag
