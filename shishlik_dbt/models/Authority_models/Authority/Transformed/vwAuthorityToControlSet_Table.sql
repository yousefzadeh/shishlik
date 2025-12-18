-- Main table 
select distinct
    dp.Tenant_Id,
    -- Filters
    dp.Authority_Id,
    dp.Authority_Name,
    dc.Controlset_Id,
    dc.Controlset_Name,
    -- Table
    dp.AuthorityProvision_Id,
    dp.AuthorityProvision_ReferenceId,
    dp.AuthorityProvision_Name,
    -- dc.Controlset_Name,
    dc.Controls_Id,
    dc.Controls_Reference,
    dc.Controls_Name,
    dc.Controls_Detail,
    dc.ControlsetDomain_Name
from {{ ref("FactRelationProvisionToControl") }} f
right join {{ ref("DimControl") }} dc on f.Controls_Id = dc.Controls_Id
right join
    {{ ref("DimProvision") }} dp on f.AuthorityProvision_Id = dp.AuthorityProvision_Id and f.Tenant_Id = dp.Tenant_Id
where
    1 = 1 and coalesce(dc.Controlset_IsCurrent, 1) = 1
    -- where dp.Tenant_Id = 3 and dp.Authority_Id = 1
    -- where dp.Tenant_Id = 3 and dp.Authority_Id = 11
    -- where dp.Tenant_Id = 3 and dp.Authority_Id = 10 -- 6clicks 33 rows
    -- where dp.Tenant_Id = 3 and dp.Authority_Id = 4 -- 6clicks 771 rows
    -- where dp.Tenant_Id = 3 and dp.Authority_Id = 5 -- 6clicks 11 rows
    -- where dp.Tenant_Id = 3 and dp.Authority_Id = 12-- 6clicks 461 rows -- long...20s
    -- where dp.Tenant_Id = 3 and dp.Authority_Id = 122 -- 6clicks 108 rows
    -- where dp.Tenant_Id = 3 and dp.Authority_Id = 6 -- 6clicks 535 rows
    -- where dp.Tenant_Id = 3 and dp.Authority_Id = 5 -- 6clicks 11 rows
    -- where dp.Tenant_Id = 3 and dp.Authority_Id = 19 -- 6clicks 282 rows
    -- where dp.Tenant_Id = 3 and dp.Authority_Id = 157 -- 6clicks 7 rows
    
