-- List of all Tenant and Authorities that are created or downloaded without duplicates.
-- If an Authority is both created and downloaded from the same tenant then only downloaded is listed. 
with
    created_auth as (
        select a.Id Authority_Id, a.TenantId Tenant_Id, a.NameVarchar Authority_Name, getdate() Authority_UpdateTime
        -- Coalesce(LastModificationTime,CreationTime) Authority_UpdateTime
        from {{ source("assessment_models", "Authority") }} a
        where IsDeleted = 0 and IsArchived = 0
    ),
    downloaded_auth as (
        -- There are duplicates in the downloaded table, so we need to remove them 
        select ta.AuthorityId Authority_Id, ta.TenantId Tenant_Id, a.Authority_Name, getdate() Authority_UpdateTime
        -- Coalesce(ta.LastModificationTime,ta.CreationTime) Authority_UpdateTime 
        from {{ source("tenant_models", "TenantAuthority") }} ta
        join created_auth a on ta.AuthorityId = a.Authority_Id and ta.IsDeleted = 0
        where ta.IsDeleted = 0 and ta.IsArchived = 0
    ),
    un as (
        select c.Authority_Id, c.Tenant_Id, c.Authority_Name, 'Created' Method, c.Authority_UpdateTime
        from created_auth c
        union all
        select d.Authority_Id, d.Tenant_Id, d.Authority_Name, 'Marketplace' Method, d.Authority_UpdateTime
        from downloaded_auth d
    ),
    final as (
    -- Group by - no duplicates
    select
        un.Tenant_Id,
        un.Authority_Id,
        un.Authority_Name,
        max(un.Method) Method,  -- if both Methods are different then choose Downloaded 
        max(un.Authority_UpdateTime) Authority_UpdateTime  -- choose the latest update time 
    from un
    group by un.Tenant_Id, un.Authority_Id, un.Authority_Name
    )
select 
Tenant_Id,
Authority_Id,
Authority_Name,
Method,
Authority_UpdateTime
from final
{# where Tenant_Id = 1384 #}
