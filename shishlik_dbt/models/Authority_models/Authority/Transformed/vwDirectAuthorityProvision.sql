-- List of all Tenant and Authorities that are created or downloaded without duplicates.
-- If an Authority is both created and downloaded from the same tenant then only downloaded is listed. 
with
    created_auth_prov as (
        select 
        a.TenantId Tenant_Id, 
        a.Id Authority_Id, 
        a.NameVarchar Authority_Name, 
        ap.Id AuthorityProvision_Id,
        ap.ReferenceId AuthorityProvision_ReferenceId,
        ap.Name AuthorityProvision_Name
        from {{ source("assessment_models", "Authority") }} a
        join {{ source("assessment_models", "AuthorityProvision") }} ap 
          on a.Id = ap.AuthorityId and ap.IsDeleted = 0 and a.IsDeleted = 0 and a.IsArchived = 0
    ),
    downloaded_auth_prov as (
        -- There are duplicates in the downloaded table, so we need to remove them 
        select 
        ta.TenantId Tenant_Id, 
        a.Authority_Id, 
        a.Authority_Name, 
        a.AuthorityProvision_Id,
        a.AuthorityProvision_ReferenceId,
        a.AuthorityProvision_Name
        from {{ source("tenant_models", "TenantAuthority") }} ta
        join created_auth_prov a on ta.AuthorityId = a.Authority_Id and ta.IsDeleted = 0 and ta.IsArchived = 0
    ),
    un as (
        select 
        c.Authority_Id, 
        c.Tenant_Id, 
        c.Authority_Name,
        c.AuthorityProvision_Id,
        c.AuthorityProvision_ReferenceId,
        c.AuthorityProvision_Name, 
        'Created' Method
        from created_auth_prov c
        union all
        select 
        d.Authority_Id, 
        d.Tenant_Id, 
        d.Authority_Name,
        d.AuthorityProvision_Id,
        d.AuthorityProvision_ReferenceId,
        d.AuthorityProvision_Name, 
        'Marketplace' Method
        from downloaded_auth_prov d
    ),
    final as (
    -- Group by - no duplicates
    select
        un.Tenant_Id,
        un.Authority_Id,
        max(un.Authority_Name) Authority_Name,
        un.AuthorityProvision_Id,
        max(un.AuthorityProvision_ReferenceId) AuthorityProvision_ReferenceId,
        max(un.AuthorityProvision_Name) AuthorityProvision_Name, 
        max(un.Method) Method  -- if both Methods are different then choose Downloaded 
    from un
    group by un.Tenant_Id, un.Authority_Id, un.AuthorityProvision_Id
    )
select 
Tenant_Id,
Authority_Id,
Authority_Name,
AuthorityProvision_Id,
AuthorityProvision_ReferenceId,
AuthorityProvision_Name,
Method
from final
{# where Tenant_Id = 1384 #}
