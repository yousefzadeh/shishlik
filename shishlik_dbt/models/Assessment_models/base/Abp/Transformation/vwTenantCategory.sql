-- List of Tenants and its Category - Hub, Spoke, Stand-Alone
-- Tenant in AbpTenants table that are not deleted and is not used as a template
with
    all_tenants as (
        select t.Id Tenant_Id from {{ source("assessment_models", "AbpTenants") }} t where t.IsTemplate = 0  -- All tenants that are not templates
    ),
    hub_spoke_plan as (

        select t.Id as Tenant_Id
        from {{ source("assessment_models", "AbpTenants") }} t
        join {{ source("assessment_models", "AbpEditions") }} ae on t.EditionId = ae.Id
        where ae.IsServiceProviderEdition = 1  -- hub and spoke subscription plan

    ),
    hub_spoke_tenants as (
        -- List of Hub tenants
        select tv.TenantId Tenant_Id, 'Hub' Tenant_Type
        from {{ source("tenant_models", "TenantVendor") }} tv
        join hub_spoke_plan t on tv.TenantId = t.Tenant_Id
        where
            tv.TenantId = tv.VendorId  -- condition for hub tenant
            and tv.isarchived = 0  -- Only want tenants not archived

        union all

        -- List of spoke tenants
        select tv.VendorId Tenant_Id, 'Spoke' Tenant_Type
        from {{ source("tenant_models", "TenantVendor") }} tv
        join hub_spoke_plan t on tv.TenantId = t.Tenant_Id
        where
            tv.TenantId <> tv.VendorId  -- condition for spoke tenant
            and tv.isarchived = 0  -- Only want tenants not archived
    ),
    standalone_tenants as (
        -- List of Stand-Alone tenants are the remaining tenants that are not hub or spoke
        select Tenant_Id, 'Stand-Alone' Tenant_Type
        from
            (
                select Tenant_Id
                from all_tenants
                except
                select Tenant_Id
                from hub_spoke_tenants
            ) as T
    )
select *
from hub_spoke_tenants
union all
select *
from standalone_tenants
