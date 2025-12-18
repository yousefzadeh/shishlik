-- Authority to Control Set compliance
-- All controls
with
    joel_all_controls as (

        select
            ap.AuthorityPolicy_TenantId Tenant_Id,
            ap.AuthorityPolicy_AuthorityId Authority_Id,
            a.Authority_Name,
            ap.AuthorityPolicy_PolicyId ControlSet_Id,
            p.Policy_Name ControlSet_Name,
            c.Controls_Id
        from {{ ref("vwAuthority") }} a
        join {{ ref("vwAuthorityPolicy") }} ap on a.Authority_Id = ap.AuthorityPolicy_AuthorityId
        join {{ ref("vwPolicy") }} p on ap.AuthorityPolicy_PolicyId = p.Policy_Id
        join {{ ref("vwPolicyDomain") }} pd on p.Policy_Id = pd.PolicyDomain_PolicyId
        join {{ ref("vwControls") }} c on pd.PolicyDomain_Id = c.Controls_PolicyDomainId
        where p.Policy_IsCurrent = 1
    -- and ap.AuthorityPolicy_TenantId = 3
    -- and ap.AuthorityPolicy_AuthorityId = 10  -- 1127 rows
    -- and ap.AuthorityPolicy_PolicyId in (@listofpolicyids)
    ),
    joel_mapped_controls as (

        select
            ap.AuthorityPolicy_TenantId Tenant_Id,
            ap1.AuthorityProvision_AuthorityId Authority_Id,
            a.Authority_Name,
            pd.PolicyDomain_PolicyId ControlSet_Id,
            p.Policy_Name ControlSet_Name,
            c.Controls_Id
        from {{ ref("vwAuthority") }} a
        join {{ ref("vwAuthorityPolicy") }} ap on a.Authority_Id = ap.AuthorityPolicy_AuthorityId
        join {{ ref("vwPolicy") }} p on ap.AuthorityPolicy_PolicyId = p.Policy_Id
        join {{ ref("vwPolicyDomain") }} pd on p.Policy_Id = pd.PolicyDomain_PolicyId
        join {{ ref("vwControls") }} c on pd.PolicyDomain_Id = c.Controls_PolicyDomainId
        join {{ ref("vwProvisionControl") }} pc on c.Controls_Id = pc.ProvisionControl_ControlsId
        join
            {{ ref("vwAuthorityProvision") }} ap1
            on pc.ProvisionControl_AuthorityReferenceId = ap1.AuthorityProvision_Id
        where ap.AuthorityPolicy_AuthorityId = ap1.AuthorityProvision_AuthorityId and p.Policy_IsCurrent = 1
    -- and ap.AuthorityPolicy_TenantId = 3
    -- and ap1.AuthorityProvision_AuthorityId = 10 -- 35 rows 
    -- and pd.PolicyDomain_PolicyId in (@listofpolicyids)
    ),
    joel_all_provisions as (

        select t.Tenant_Id, ap.AuthorityProvision_AuthorityId Authority_Id, ap.AuthorityProvision_Id
        from {{ ref("vwAuthorityProvision") }} ap
        join
            (
                select distinct *
                from
                    (
                        select TenantAuthority_AuthorityId Authority_Id, ta.TenantAuthority_TenantId Tenant_Id
                        from {{ ref("vwTenantAuthority") }} ta
                        join {{ ref("vwAuthority") }} a on a.Authority_Id = ta.TenantAuthority_AuthorityId
                        union all
                        -- Tenant Id from Tenant Table
                        select Authority_Id, Authority_TenantId
                        from {{ ref("vwAuthority") }} a
                    ) as u
            ) t
            on ap.AuthorityProvision_AuthorityId = t.Authority_Id
    -- where ap.AuthorityProvision_AuthorityId = 12 and t.Tenant_Id = 1384 -- 114 rows and distinct 
    ),
    joel_mapped_provisions as (

        select
            pc.ProvisionControl_TenantId Tenant_Id,
            -- Filters
            ap.AuthorityProvision_AuthorityId Authority_Id,
            pd.PolicyDomain_PolicyId Controlset_Id,
            ap.AuthorityProvision_Id
        from {{ ref("vwAuthorityProvision") }} ap
        join {{ ref("vwProvisionControl") }} pc on ap.AuthorityProvision_Id = pc.ProvisionControl_AuthorityReferenceId
        join {{ ref("vwControls") }} c on pc.ProvisionControl_ControlsId = c.Controls_Id
        join {{ ref("vwPolicyDomain") }} pd on c.Controls_PolicyDomainId = pd.PolicyDomain_Id
        join {{ ref("vwPolicy") }} p on pd.PolicyDomain_PolicyId = p.Policy_Id
        where p.Policy_IsCurrent = 1
    -- and ap.AuthorityProvision_AuthorityId = 12 and pc.ProvisionControl_TenantId = 1384 -- 2401 rows, 126 distinct
    -- rows, 104 distinct provision_Id
    -- and pd.PolicyDomain_PolicyId in (@listofpolicyids)
    ),
    all_controls as (
        select distinct
            ac.Tenant_Id,
            -- Filters
            ac.Authority_Id,
            ac.Authority_Name,
            ac.Controlset_Id,
            ac.Controlset_Name,
            ac.Controls_Id all_controls,
            NULL mapped_controls,
            NULL all_provisions,
            NULL mapped_provisions
        from joel_all_controls ac
    -- AND ac.Tenant_Id = 3 AND ac.Authority_Id = 10
    ),
    -- Mapped controls
    mapped_controls as (
        select distinct
            mc.Tenant_Id,
            -- Filters
            mc.Authority_Id,
            mc.Authority_Name,
            mc.Controlset_Id,
            mc.Controlset_Name,
            NULL all_controls,
            mc.Controls_Id mapped_controls,
            NULL all_provisions,
            NULL mapped_provisions
        from joel_mapped_controls mc
    -- AND dp.Tenant_Id = 3 AND dp.Authority_Id = 10
    ),
    -- All Provisions across Control sets
    all_provisions as (

        select
            ap.Tenant_Id,
            -- Filters
            ap.Authority_Id,
            a.Authority_Name,
            NULL Controlset_Id,
            NULL Controlset_Name,
            NULL all_controls,
            NULL mapped_controls,
            ap.AuthorityProvision_Id all_provisions,
            NULL mapped_provisions
        from joel_all_provisions ap
        join {{ ref("vwAuthority") }} a on ap.Authority_Id = a.Authority_Id
        where 1 = 1
    -- AND t.Tenant_Id = 1384 AND ap.Authority_Id = 12
    ),
    -- Mapped Provisions 
    mapped_provisions as (

        select distinct
            mp.Tenant_Id,
            -- Filters
            mp.Authority_Id,
            a.Authority_Name,
            mp.Controlset_Id,
            p.Policy_Name Controlset_Name,
            NULL all_controls,
            NULL mapped_controls,
            NULL all_provisions,
            mp.AuthorityProvision_Id mapped_provisions
        from joel_mapped_provisions mp
        join {{ ref("vwAuthority") }} a on mp.Authority_Id = a.Authority_Id
        join {{ ref("vwPolicy") }} p on mp.Controlset_Id = p.Policy_Id
        where 1 = 1
    ),
    uni as (
        select *
        from all_controls
        union all
        select *
        from mapped_controls
        union all
        select *
        from all_provisions
        union all
        select *
        from mapped_provisions
    )

select *
from uni
