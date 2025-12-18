with base as(
select 
r.TenantId tenant_id,
r.Id id,
r.TenantEntityUniqueId IdRef,
r.Name record_name,
r.Description Description,
r.CommonCause,
r.LikelyImpact, 
r.CreationTime CreationDate,
case
when [TreatmentStatusId] = 1 then 'Draft'
when [TreatmentStatusId] = 2 then 'Approved'
when [TreatmentStatusId] = 3 then 'Treatment in progress'
when [TreatmentStatusId] = 4 then 'Treatment paused'
when [TreatmentStatusId] = 5 then 'Treatment cancelled'
when [TreatmentStatusId] = 6 then 'Treatment completed'
when [TreatmentStatusId] = 7 then 'Closed'
else 'Undefined' end as TreatmentStatus,
sl.Name TreatmentDecision,
tpa.LabelVarchar RiskRating

from {{ source("hailey_models", "Risk") }} r
left join {{ source("hailey_models", "StatusLists") }} sl on sl.Id = r.TreatmentDecisionId and sl.IsDeleted = 0
left join {{ source("hailey_models", "ThirdPartyAttributes") }} tpa on tpa.Id = r.RiskRatingId and tpa.IsDeleted = 0
where r.IsDeleted = 0 and r.Status = 1
)
, rtag as (
select rtg.TenantId, rtg.RiskId, STRING_AGG(t.Name, ', ') Tag
from {{ source("hailey_models", "RiskTag") }} rtg
join {{ source("hailey_models", "Tags") }} t on rtg.TagId = t.Id and rtg.TenantId = t.TenantId and t.IsDeleted = 0
where rtg.IsDeleted = 0
group by rtg.RiskId, rtg.TenantId
)
, rowner_base as (
select ro.TenantId, ro.RiskId, au.Name+' '+au.Surname Owner
from {{ source("hailey_models", "RiskOwner") }} ro
left join {{ source("hailey_models", "AbpUsers") }} au on ro.UserId = au.Id and au.IsDeleted = 0
where ro.IsDeleted = 0

union all

select ro.TenantId, ro.RiskId, aou. DisplayName Owner
from {{ source("hailey_models", "RiskOwner") }} ro
left join {{ source("hailey_models", "AbpOrganizationUnits") }} aou on ro.OrganizationUnitId = aou.Id and aou.IsDeleted = 0
where ro.IsDeleted = 0
)
, rowner as (
select ro.TenantId, ro.RiskId, STRING_AGG(ro.owner, ', ') Owner
from rowner_base ro
group by ro.TenantId, ro.RiskId
)
, raccmem_base as (
select ru.TenantId, ru.RiskId, au.Name+' '+au.Surname access_members
from {{ source("hailey_models", "RiskUser") }} ru
left join {{ source("hailey_models", "AbpUsers") }} au on ru.UserId = au.Id and au.IsDeleted = 0
where ru.IsDeleted = 0

union all

select ru.TenantId, ru.RiskId, aou. DisplayName access_members
from {{ source("hailey_models", "RiskUser") }} ru
left join {{ source("hailey_models", "AbpOrganizationUnits") }} aou on ru.OrganizationUnitId = aou.Id and aou.IsDeleted = 0
where ru.IsDeleted = 0
)
, raccmem as (
select ru.TenantId, ru.RiskId, STRING_AGG(ru.access_members, ', ') AccessMembers
from raccmem_base ru
group by ru.TenantId, ru.RiskId
)
, ris_dom as(
select
rcad.RiskId, tpa.LabelVarchar as Domain
from {{ source("hailey_models", "RiskCustomAttributeData") }} rcad
join {{ source("hailey_models", "ThirdPartyAttributes") }} tpa
on tpa.Id = rcad.ThirdPartyAttributesId and tpa.IsDeleted = 0
join {{ source("hailey_models", "ThirdPartyControl") }} tpc
on tpc.Id = tpa.ThirdPartyControlId and tpc.Name = 'RiskDomain' and tpc.IsDeleted = 0
where rcad.IsDeleted = 0 and tpc.Enabled = 1 and tpc.EntityType = 2
)

select
r.tenant_id,
r.id,
r.IdRef,
r.record_name Name,
r.Description,
r.CommonCause,
r.LikelyImpact,
r.CreationDate,
rd.Domain,
r.TreatmentStatus,
r.TreatmentDecision,
r.RiskRating,
rtg.Tag,
ro.Owner,
ru.AccessMembers

from base r
left join rtag rtg on rtg.RiskId = r.id
left join rowner ro on ro.RiskId = r.id
left join raccmem ru on ru.RiskId = r.id
left join ris_dom rd on rd.RiskId = r.id