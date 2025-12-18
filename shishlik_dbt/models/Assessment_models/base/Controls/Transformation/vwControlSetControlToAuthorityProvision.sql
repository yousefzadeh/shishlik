with
    created_auth as (
        select a.Id Authority_Id, a.TenantId Tenant_Id
        from {{ source("assessment_models", "Authority") }} a
        where IsDeleted = 0
    ),
    downloaded_auth as (
        select ta.AuthorityId Authority_Id, ta.TenantId Tenant_Id
        from {{ source("tenant_models", "TenantAuthority") }} ta
        join created_auth a on ta.AuthorityId = a.Authority_Id and ta.IsDeleted = 0
    ),
    union_auth as (
        select c.Authority_Id, c.Tenant_Id
        from created_auth c
        union all
        select d.Authority_Id, d.Tenant_Id
        from downloaded_auth d
    ),
    auth as (
        -- Remove duplicates
        select un.Tenant_Id, un.Authority_Id
        from union_auth un
        group by un.Tenant_Id, un.Authority_Id
    ),
    controlset_control as (
        -- Control Set - Control Hierarchy
        select pol.TenantId Tenant_Id, pol.Id Policy_Id, c.Id Control_Id
        from {{ source("assessment_models", "Policy") }} pol
        join
            {{ source("assessment_models", "PolicyDomain") }} pd on pd.PolicyId = pol.Id
        join {{ source("assessment_models", "Controls") }} c on c.PolicyDomainId = pd.Id
        where
            pol.TenantId = c.TenantId
            and pol.IsDeleted = 0
            and pd.IsDeleted = 0
            and c.IsDeleted = 0  -- check for accuracy of cascading delete
    ),
    auth_prov as (
        -- Authority - Provision Hierarchy
        select auth.Tenant_Id, auth.Authority_Id, ap.Id AuthorityProvision_Id
        from auth
        join
            {{ source("assessment_models", "AuthorityProvision") }} ap
            on auth.Authority_Id = ap.AuthorityId
        where auth.Tenant_Id = ap.TenantId and ap.IsDeleted = 0  -- check for accuracy of cascading delete
    ),
    csc_to_ap as (
        -- Control Set - Authority Provision Hierarchy
        select
            csc.Tenant_Id,
            csc.Policy_Id ControlSet_Id,
            csc.Control_Id,
            ap.Authority_Id,
            ap.AuthorityProvision_Id
        from controlset_control csc
        left join
            {{ source("assessment_models", "AuthorityPolicy") }} auth_pol
            on auth_pol.PolicyId = csc.Policy_Id
            and auth_pol.IsDeleted = 0
            and csc.Tenant_Id = auth_pol.TenantId
        left join
            {{ source("assessment_models", "ProvisionControl") }} prov_con
            on prov_con.ControlsId = csc.Control_Id
            and prov_con.IsDeleted = 0
            and csc.Tenant_Id = prov_con.TenantId
        left join
            auth_prov ap
            on ap.Authority_Id = auth_pol.AuthorityId
            and ap.AuthorityProvision_Id = prov_con.AuthorityReferenceId
    -- and ap.Tenant_Id = auth_pol.TenantId
    -- and ap.Tenant_Id = prov_con.TenantId
    ),
    final as (
        select Tenant_Id, ControlSet_Id, Control_Id, Authority_Id, AuthorityProvision_Id
        from csc_to_ap
        group by
            Tenant_Id, ControlSet_Id, Control_Id, Authority_Id, AuthorityProvision_Id
    )
select *
from final
