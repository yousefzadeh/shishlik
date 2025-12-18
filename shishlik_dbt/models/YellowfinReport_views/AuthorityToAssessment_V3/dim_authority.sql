-- One row per Authority on Authority_Id
-- List of all Tenant and Authorities that are created or downloaded without duplicates.
-- If an Authority is both created and downloaded from the same tenant then only downloaded is listed. 
with
    created_auth as (
        select a.Id Authority_Id, a.TenantId Tenant_Id, a.NameVarchar Authority_Name, a.Name Authority_NameLabel
        from {{ source("assessment_models", "Authority") }} a
    ),
    downloaded_auth as (
        -- There are duplicates in the downloaded table, so we need to remove them 
        select distinct
            ta.AuthorityId Authority_Id, ta.TenantId Tenant_Id, a.NameVarchar Authority_Name, a.Name Authority_NameLabel
        from {{ source("tenant_models", "TenantAuthority") }} ta
        join {{ source("assessment_models", "Authority") }} a on ta.AuthorityId = a.Id
    ),
    un as (
        select c.Authority_Id, c.Tenant_Id, c.Authority_Name, c.Authority_NameLabel, 'Created' Method
        from created_auth c
        union all
        select d.Authority_Id, d.Tenant_Id, d.Authority_Name, d.Authority_NameLabel, 'Downloaded' Method
        from downloaded_auth d
    ),
    auth_assigned as (
        -- Group by - no duplicates
        select un.Tenant_Id, un.Authority_Id, un.Authority_Name, un.Authority_NameLabel, max(un.Method) Method  {# if both Methods are different then choose Downloaded  #}
        from un
        group by un.Tenant_Id, un.Authority_Id, un.Authority_Name, un.Authority_NameLabel
    ),
    auth_zero as (
        select
            0 Authority_Id,
            Id Tenant_Id,
            'Unassigned Authority' Authority_Name,
            'Unassigned Authority' Authority_NameLabel,
            'No Authority' Method
        from {{ source("assessment_models", "AbpTenants") }}
    ),
    final as (
        select *
        from auth_assigned
        union all
        select *
        from auth_zero
    )
select *
from final
