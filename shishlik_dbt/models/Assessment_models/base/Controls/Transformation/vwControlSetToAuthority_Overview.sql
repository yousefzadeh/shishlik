-- Authority to Control Set compliance
-- Base queries from Joel
with
    joel_all_controls as (

        select c.Controls_TenantId Tenant_Id, pd.PolicyDomain_PolicyId ControlSet_Id, c.Controls_Id
        from {{ ref("vwControls") }} c
        join {{ ref("vwPolicyDomain") }} pd on c.Controls_PolicyDomainId = pd.PolicyDomain_Id
    -- where Controls_TenantId =1384 and pd.PolicyDomain_PolicyId = 2952 -- 713 rows and distinct
    ),
    -- Mapped controls
    joel_mapped_controls as (

        select
            c.Controls_TenantId Tenant_Id,
            pd.PolicyDomain_PolicyId ControlSet_Id,
            ap.AuthorityProvision_AuthorityId Authority_Id,
            c.Controls_Id
        from {{ ref("vwControls") }} c
        join {{ ref("vwPolicyDomain") }} pd on c.Controls_PolicyDomainId = pd.PolicyDomain_Id
        join {{ ref("vwProvisionControl") }} pc on c.Controls_Id = pc.ProvisionControl_ControlsId
        join {{ ref("vwAuthorityProvision") }} ap on pc.ProvisionControl_AuthorityReferenceId = ap.AuthorityProvision_Id
    -- where Controls_TenantId =1384 and pd.PolicyDomain_PolicyId = 2952 -- 2376 rows and 541 distinct
    ),
    joel_all_provisions as (

        select distinct
            ap.AuthorityPolicy_TenantId Tenant_Id,
            ap.AuthorityPolicy_PolicyId Controlset_Id,
            ap.AuthorityPolicy_AuthorityId Authority_Id,
            ap1.AuthorityProvision_Id
        from {{ ref("vwAuthorityPolicy") }} ap
        join {{ ref("vwAuthority") }} a on ap.AuthorityPolicy_AuthorityId = a.Authority_Id
        join {{ ref("vwAuthorityProvision") }} ap1 on a.Authority_Id = ap1.AuthorityProvision_AuthorityId
    -- where ap.AuthorityPolicy_TenantId = 1384 and ap.AuthorityPolicy_PolicyId = 2952 -- 188 rows and distinct
    -- and ap.AuthorityPolicy_AuthorityId in (@listofauthorityids)
    ),
    joel_mapped_provisions as (

        select
            ap.AuthorityPolicy_TenantId Tenant_Id,
            ap.AuthorityPolicy_PolicyId ControlSet_Id,
            ap.AuthorityPolicy_AuthorityId Authority_Id,
            ap1.AuthorityProvision_Id
        from {{ ref("vwAuthorityPolicy") }} ap
        join {{ ref("vwAuthority") }} a on ap.AuthorityPolicy_AuthorityId = a.Authority_Id
        join {{ ref("vwAuthorityProvision") }} ap1 on a.Authority_Id = ap1.AuthorityProvision_AuthorityId
        join {{ ref("vwProvisionControl") }} pc on ap1.AuthorityProvision_Id = pc.ProvisionControl_AuthorityReferenceId
        join {{ ref("vwControls") }} c on pc.ProvisionControl_ControlsId = c.Controls_Id
        join {{ ref("vwPolicyDomain") }} pd on c.Controls_PolicyDomainId = pd.PolicyDomain_Id
        where ap.AuthorityPolicy_PolicyId = pd.PolicyDomain_PolicyId
    -- and ap.AuthorityPolicy_TenantId = 1384 and ap.AuthorityPolicy_PolicyId = 2952 -- 2376 rows and 102 distinct
    ),
    -- All Controls across all authorities
    all_controls as (
        select
            c.Tenant_Id,
            -- Filters
            c.Controlset_Id,
            p.Policy_Name ControlSet_Name,
            p.Policy_VersionDate Controlset_Version,
            p.Policy_PublishedDate Controlset_PublishedDate,
            p.Policy_IsCurrent Controlset_IsCurrent,
            NULL Authority_Id,
            NULL Authority_Name,
            c.Controls_Id all_controls,
            NULL mapped_controls,
            NULL all_provisions,
            NULL mapped_provisions
        from joel_all_controls c
        join {{ ref("vwPolicy") }} p on c.ControlSet_Id = p.Policy_Id
        where 1 = 1
    -- AND dc.Controlset_StatusCode = 'Published'
    -- AND c.Tenant_Id = 1384 AND p.Policy_Id = 2952 -- 713 rows and distinct
    ),
    -- Mapped controls
    mapped_controls as (
        select distinct
            mc.Tenant_Id,
            -- Filters
            mc.Controlset_Id,
            p.Policy_Name Controlset_Name,
            p.Policy_VersionDate Controlset_Version,
            p.Policy_PublishedDate Controlset_PublishedDate,
            p.Policy_IsCurrent Controlset_IsCurrent,
            mc.Authority_Id,
            a.Authority_Name,
            NULL all_controls,
            mc.Controls_Id mapped_controls,
            NULL all_provisions,
            NULL mapped_provisions
        from joel_mapped_controls mc
        join {{ ref("vwPolicy") }} p on mc.Controlset_Id = p.Policy_Id
        join {{ ref("vwAuthority") }} a on mc.Authority_Id = a.Authority_Id
        where 1 = 1
    -- AND dc.Controlset_StatusCode = 'Published'
    -- AND mc.Tenant_Id = 1384 AND mc.ControlSet_Id = 2952 -- 541 rows and distinct
    ),
    -- All Provisions across Control sets
    all_provisions as (
        select  -- DISTINCT
            p.Tenant_Id,
            -- Filters
            p.Controlset_Id,
            cs.Policy_Name Controlset_Name,
            cs.Policy_VersionDate Controlset_Version,
            cs.Policy_PublishedDate Controlset_PublishedDate,
            cs.Policy_IsCurrent Controlset_IsCurrent,
            p.Authority_Id,
            a.Authority_Name,
            NULL all_controls,
            NULL mapped_controls,
            p.AuthorityProvision_Id all_provisions,
            NULL mapped_provisions
        from joel_all_provisions p
        join {{ ref("vwAuthority") }} a on p.Authority_Id = a.Authority_Id
        join {{ ref("vwPolicy") }} cs on p.Controlset_Id = cs.Policy_Id
        where 1 = 1
    -- AND dc.Controlset_StatusCode = 'Published'
    -- AND p.Tenant_Id = 1384 AND p.ControlSet_Id = 2952 -- 188 rows and distinct
    ),
    -- Mapped Provisions 
    mapped_provisions as (
        select distinct
            mp.Tenant_Id,
            -- Filters
            mp.Controlset_Id,
            cs.Policy_Name Controlset_Name,
            cs.Policy_VersionDate Controlset_Version,
            cs.Policy_PublishedDate Controlset_PublishedDate,
            cs.Policy_IsCurrent Controlset_IsCurrent,
            mp.Authority_Id,
            a.Authority_Name,
            NULL all_controls,
            NULL mapped_controls,
            NULL all_provisions,
            mp.AuthorityProvision_Id mapped_provisions
        from joel_mapped_provisions mp
        join {{ ref("vwAuthority") }} a on mp.Authority_Id = a.Authority_Id
        join {{ ref("vwPolicy") }} cs on mp.Controlset_Id = cs.Policy_Id
        where 1 = 1
    -- AND dc.Controlset_StatusCode = 'Published'
    -- AND mp.Tenant_Id = 1384 AND mp.ControlSet_Id = 2952 -- 2376 rows and 102 distinct
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

select
    Tenant_Id,
    -- Filters
    Controlset_Id,
    Controlset_Name,
    Cast(Controlset_Version as varchar) Controlset_Version,
    cast(format(Controlset_PublishedDate, 'dd MMM, yyyy') as varchar) Controlset_PublishedDate,
    Controlset_IsCurrent,
    Authority_Id,
    Authority_Name,
    all_controls,
    mapped_controls,
    all_provisions,
    mapped_provisions
from uni
