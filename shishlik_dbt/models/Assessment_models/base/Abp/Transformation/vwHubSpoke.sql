-- Relation of Hub to Spoke Tenants
with 
  tenant as (
    select 
    Id AbpTenants_Id,
    EditionId AbpTenants_EditionId
    from {{ source("assessment_models","AbpTenants") }}
    where IsDeleted = 0 and IsActive = 1
  ),
  edition as (
    select 
    Id AbpEditions_Id,
    [Name] AbpEditions_Name,
    IsServiceProviderEdition
    from {{ source("assessment_models","AbpEditions") }}
    where IsDeleted = 0
  ),
  tenant_vendor as (
    select 
    TenantId,
    VendorId
    from {{ source("tenant_models", "TenantVendor") }}
    where IsDeleted = 0 and isArchived = 0
  )
select tv.TenantId Hub_TenantId, tv.VendorId Spoke_TenantId
from tenant_vendor tv
join tenant hub on tv.TenantId = hub.AbpTenants_Id
join tenant spoke on tv.VendorId = spoke.AbpTenants_Id
join edition e on hub.AbpTenants_EditionId = e.AbpEditions_Id
where e.IsServiceProviderEdition = 1  -- Only Hub tenants
