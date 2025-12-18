with base as(
    select 
        iss.TenantId tenant_id,
        iss.Id id,
        iss.CreationTime CreationDate,
        iss.Name record_name,
        iss.Description Description,
        ah.HostIPOrDomain as HostIP,
        ad.HostIPOrDomain as Domain
    from {{ source("hailey_models", "Issues") }} iss
    join {{ source("hailey_models", "EntityRegister") }} er
        on er.Id = iss.EntityRegisterId and er.IsDeleted = 0 and er.EntityType = 5  
    left join {{ source("hailey_models", "AssetHostIPOrDomain") }} ah
        on ah.HostIPRegisterItemId  = iss.Id and ah.IsDeleted = 0
    left join {{ source("hailey_models", "AssetHostIPOrDomain") }} ad
        on ad.DomainRegisterItemId  = iss.Id and ad.IsDeleted = 0
    where iss.IsDeleted = 0
    and iss.Status != 100
)
, ast_type as (
    select 
        ic.TenantId,
        ic.IssueId,
        ic.ThirdPartyAttributesId,
        tpa.Id,
        tpa.Label Type
    from {{ source("hailey_models", "IssueCustomAttributeData") }} ic
    left join {{ source("hailey_models", "ThirdPartyAttributes") }} tpa  
        on tpa.Id = ic.ThirdPartyAttributesId and tpa.IsDeleted = 0
    left join {{ source("hailey_models", "ThirdPartyControl") }} tpc 
        on tpc.Id = tpa.ThirdPartyControlId and tpc.IsDeleted = 0
    where tpc.Name = 'Type'
)
, ast_tag as (
    select 
        it.TenantId, 
        it.IssueId, 
        STRING_AGG(t.Name, ', ') tag
    from {{ source("hailey_models", "IssueTag") }} it
    join {{ source("hailey_models", "Tags") }} t 
        on it.TagId = t.Id and it.TenantId = t.TenantId and t.IsDeleted = 0
    where it.IsDeleted = 0
    group by it.TenantId, it.IssueId
)
, ast_tp as (
    select 
        at.tenant_id, 
        at.id, 
        STRING_AGG(tv.Name, ', ') third_party
    from base at
    left join {{ source("hailey_models", "IssueThirdParty") }} itp 
        on itp.TenantVendorId = at.id and itp.IsDeleted = 0
    left join {{ source("hailey_models", "TenantVendor") }} tv 
        on tv.Id = itp.TenantVendorId and tv.IsDeleted = 0
    group by at.tenant_id, at.id
)
, ast_owner_base as (
    select 
        io.TenantId, 
        io.IssueId, 
        au.Name+' '+au.Surname owner
    from {{ source("hailey_models", "IssueOwner") }} io
    left join {{ source("hailey_models", "AbpUsers") }} au  
        on io.UserId = au.Id and au.IsDeleted = 0
    where io.IsDeleted = 0

    union all

    select 
        io.TenantId, 
        io.IssueId, 
        aou. DisplayName owner
    from {{ source("hailey_models", "IssueOwner") }} io
    left join {{ source("hailey_models", "AbpOrganizationUnits") }} aou 
        on io.OrganizationUnitId = aou.Id and aou.IsDeleted = 0
    where io.IsDeleted = 0
)
, ast_owner as (
    select 
        ao.TenantId, 
        ao.IssueId, 
        STRING_AGG(ao.owner, ', ') owner
    from ast_owner_base ao
    group by ao.TenantId, ao.IssueId
)
, ast_accmem_base as (
    select 
        iu.TenantId, 
        iu.IssueId, 
        au.Name+' '+au.Surname access_members
    from {{ source("hailey_models", "IssueUser") }} iu
    left join {{ source("hailey_models", "AbpUsers") }} au 
        on iu.UserId = au.Id and au.IsDeleted = 0
    where iu.IsDeleted = 0

    union all

    select 
        iu.TenantId, 
        iu.IssueId, 
        aou. DisplayName access_members
    from {{ source("hailey_models", "IssueUser") }} iu
    left join {{ source("hailey_models", "AbpOrganizationUnits") }} aou
        on iu.OrganizationUnitId = aou.Id and aou.IsDeleted = 0
    where iu.IsDeleted = 0
)
, ast_accmem as (
    select 
        aa.TenantId, 
        aa.IssueId, 
        STRING_AGG(aa.access_members, ', ') access_members
    from ast_accmem_base aa
    group by aa.TenantId, aa.IssueId
)

select 
    a.tenant_id,
    a.id,
    a.record_name Name,
    a.Description,
    a.HostIP,
    a.Domain,
    a.CreationDate,
    aty.type Type,
    at.tag Tag,
    atv.third_party as ThirdParty,
    ao.owner as Owner,
    aa.access_members as AccessMembers
from base a
left join ast_type aty on aty.IssueId = a.id
left join ast_tag at on at.IssueId = a.id
left join ast_owner ao on ao.IssueId = a.id
left join ast_accmem aa on aa.IssueId = a.id
left join ast_tp atv on atv.id = a.id
