with base as(
select 
i.TenantId tenant_id,
i.Id id,
i.IdRef,
i.Name record_name,
i.Description as Description,
i.ReportedBy as IdentifiedBy,
i.CreationTime CreationDate,
i.RecordedDate,
case
when [Priority] = 1 then '1 - Immediate'
when [Priority] = 2 then '2 - High'
when [Priority] = 3 then '3 - Medium'
when [Priority] = 4 then '4 - Low'
else 'Not selected' end Priority

from {{ source("hailey_models", "Issues") }} i
join {{ source("hailey_models", "EntityRegister") }} er
on er.Id = i.EntityRegisterId and er.IsDeleted = 0  and er.EntityType = 3
where i.IsDeleted = 0 and i.IsArchived = 0 and i.Status != 100
)
, iss_type as (
select i.TenantId, i.Id, tpa.LabelVarchar type
from {{ source("hailey_models", "Issues") }} i
join {{ source("hailey_models", "EntityRegister") }} er
on er.Id = i.EntityRegisterId and er.IsDeleted = 0  and er.EntityType = 3
join {{ source("hailey_models", "IssueCustomAttributeData") }} icad on i.Id = icad.IssueId and icad.IsDeleted = 0
join {{ source("hailey_models", "ThirdPartyAttributes") }} tpa on icad.ThirdPartyAttributesId = tpa.Id and tpa.IsDeleted = 0
join {{ source("hailey_models", "ThirdPartyControl") }} tpc on tpa.ThirdPartyControlId = tpc.Id and tpc.IsDeleted = 0
where tpc.EntityType = 6 and tpc.LabelVarchar in ('Type')
)
, iss_tag as (
select itg.TenantId, itg.IssueId, STRING_AGG(t.Name, ', ') tag
from {{ source("hailey_models", "IssueTag") }} itg
join {{ source("hailey_models", "Tags") }} t on itg.TagId = t.Id and itg.TenantId = t.TenantId and t.IsDeleted = 0
where itg.IsDeleted = 0
group by itg.IssueId, itg.TenantId
)
, iss_owner_base as (
select io.TenantId, io.IssueId, au.Name+' '+au.Surname owner
from {{ source("hailey_models", "IssueOwner") }} io
left join {{ source("hailey_models", "AbpUsers") }} au on io.UserId = au.Id and au.IsDeleted = 0
where io.IsDeleted = 0

union all

select io.TenantId, io.IssueId, aou. DisplayName owner
from {{ source("hailey_models", "IssueOwner") }} io
left join {{ source("hailey_models", "AbpOrganizationUnits") }} aou on io.OrganizationUnitId = aou.Id and aou.IsDeleted = 0
where io.IsDeleted = 0
)
, iss_owner as (
select io.TenantId, io.IssueId, STRING_AGG(io.owner, ', ') owner
from iss_owner_base io
group by io.TenantId, io.IssueId
)
, iss_accmem_base as (
select iu.TenantId, iu.IssueId, au.Name+' '+au.Surname access_members
from {{ source("hailey_models", "IssueUser") }} iu
left join {{ source("hailey_models", "AbpUsers") }} au on iu.UserId = au.Id and au.IsDeleted = 0
where iu.IsDeleted = 0

union all

select iu.TenantId, iu.IssueId, aou. DisplayName access_members
from {{ source("hailey_models", "IssueUser") }} iu
left join {{ source("hailey_models", "AbpOrganizationUnits") }} aou on iu.OrganizationUnitId = aou.Id and aou.IsDeleted = 0
where iu.IsDeleted = 0
)
, iss_accmem as (
select icm.TenantId, icm.IssueId, STRING_AGG(icm.access_members, ', ') AccessMembers
from iss_accmem_base icm
group by icm.TenantId, icm.IssueId
)

select
i.tenant_id,
i.id,
i.IdRef,
i.record_name Name,
i.Description,
i.IdentifiedBy,
i.CreationDate,
i.RecordedDate,
i.Priority,
it.Type,
itg.Tag,
io.Owner,
icm.AccessMembers

from base i
left join iss_type it on it.Id = i.id
left join iss_tag itg on itg.IssueId = i.id
left join iss_owner io on io.IssueId = i.id
left join iss_accmem icm on icm.IssueId = i.id