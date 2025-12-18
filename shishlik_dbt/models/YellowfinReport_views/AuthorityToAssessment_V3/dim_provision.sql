-- One row per Provision
-- Unassigned of each authority = -Authority_Id
with
    created_auth as (
        select
            a.Id Authority_Id,
            a.TenantId Tenant_Id,
            a.NameVarChar + case
                when a.IsDeleted = 1 then '(Deleted)' when a.IsArchived = 1 then '(Archived)' else ''
            end Authority_Name,  -- source authority
            a.Name Authority_NameLabel
        from {{ source("assessment_models", "Authority") }} a
    ),
    downloaded_auth as (
        -- There are duplicates in the downloaded table, so we need to remove them 
        select distinct ta.AuthorityId Authority_Id, ta.TenantId Tenant_Id, a.Authority_Name, a.Authority_NameLabel
        from {{ source("tenant_models", "TenantAuthority") }} ta
        join created_auth a on ta.AuthorityId = a.Authority_Id
    ),
    un as (
        select c.Authority_Id, c.Tenant_Id, c.Authority_Name, c.Authority_NameLabel, 'Created' Method
        from created_auth c
        union all
        select d.Authority_Id, d.Tenant_Id, d.Authority_Name, d.Authority_NameLabel, 'Downloaded' Method
        from downloaded_auth d
    ),
    direct_auth as (
        -- Group by - no duplicates
        select un.Tenant_Id, un.Authority_Id, un.Authority_Name, un.Authority_NameLabel, max(un.Method) Method  {# if both Methods are different then choose Downloaded  #}
        from un
        group by un.Tenant_Id, un.Authority_Id, un.Authority_Name, un.Authority_NameLabel
    ),
    prov as (
        select
            Id AuthorityProvision_Id,
            Name AuthorityProvision_Name,
            ReferenceId AuthorityProvision_ReferenceId,
            AuthorityId AuthorityProvision_AuthorityId,
            a.Tenant_Id
        from {{ source("assessment_models", "AuthorityProvision") }} ap
        join direct_auth a on ap.AuthorityId = a.Authority_Id
    ),
    prov_zero as (  -- Unassigned Provision for each Authority of each Tenant
        select
            - Authority_Id AuthorityProvision_Id,  -- negative authId
            'Unassigned Provision' AuthorityProvision_Name,
            '0' AuthorityProvision_ReferenceId,
            Authority_Id AuthorityProvision_AuthorityId,
            Tenant_Id
        from direct_auth
    ),
    final as (
        select *
        from prov
        union all
        select *
        from prov_zero
    )
select
    AuthorityProvision_Id Provision_Id,
    AuthorityProvision_Name Provision_Name,
    AuthorityProvision_ReferenceId Provision_ReferenceId,
    AuthorityProvision_AuthorityId Authority_Id,
    created_auth.Authority_Name,
    created_auth.Authority_NameLabel,
    final.Tenant_Id
from final
join created_auth on final.AuthorityProvision_AuthorityId = created_auth.Authority_Id
