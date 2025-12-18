select distinct
    dc.Tenant_Id,
    -- dp.Tenant_Id
    -- Filters
    dc.Controlset_Id,
    dc.Controlset_Name,
    cast(dc.Controlset_VersionDate as varchar) as Controlset_Version,
    cast(format(Controlset_PublishedDate, 'dd MMM, yyyy') as varchar) Controlset_PublishedDate,
    dc.ControlSet_IsCurrent,
    dp.Authority_Id,
    dp.Authority_Name,
    -- Table
    dc.Controls_Id,
    dc.Controls_Reference,
    dc.Controls_Name,
    dc.Controls_Detail,
    dc.ControlsetDomain_Name,
    -- dp.Authority_Name,
    dc.Controlset_Description,
    dp.AuthorityProvision_Id,
    dp.AuthorityProvision_ReferenceId,
    dp.AuthorityProvision_Name
from {{ ref("FactRelationProvisionToControl") }} f
right join
    {{ ref("DimProvision") }} dp on f.AuthorityProvision_Id = dp.AuthorityProvision_Id and f.Tenant_Id = dp.Tenant_Id
right join {{ ref("DimControl") }} dc on f.Controls_Id = dc.Controls_Id  -- AND dc.Controlset_StatusCode = 'Published'
