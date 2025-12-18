with base as(
select 
tv.TenantId tenant_id,
tv.Id id,
tv.Name record_name,
tv.ContactEmail,
tv.Website,
tv.CreationTime CreationDate,
tv.InherentRisk RiskRating,
tv.Criticality,
tv.Geography,
tv.Industry

from {{ source("hailey_models", "TenantVendor") }} tv
where tv.IsDeleted = 0 and tv.IsArchived = 0
)
, tp_tag as (
select tpt.TenantId, tpt.TenantVendorId, STRING_AGG(t.Name, ', ') Tag
from {{ source("hailey_models", "ThirdPartyTag") }} tpt
join {{ source("hailey_models", "Tags") }} t on tpt.TagId = t.Id and tpt.TenantId = t.TenantId and t.IsDeleted = 0
where tpt.IsDeleted = 0
group by tpt.TenantVendorId, tpt.TenantId
)
, tp_owner_base as (
select tvo.TenantId, tvo.TenantVendorId, au.Name+' '+au.Surname owner
from {{ source("hailey_models", "TenantVendorOwner") }} tvo
left join {{ source("hailey_models", "AbpUsers") }} au on tvo.UserId = au.Id and au.IsDeleted = 0
where tvo.IsDeleted = 0

union all

select tvo.TenantId, tvo.TenantVendorId, aou. DisplayName owner
from {{ source("hailey_models", "TenantVendorOwner") }} tvo
left join {{ source("hailey_models", "AbpOrganizationUnits") }} aou on tvo.OrganizationUnitId = aou.Id and aou.IsDeleted = 0
where tvo.IsDeleted = 0
)
, tp_owner as (
select tvo.TenantId, tvo.TenantVendorId, STRING_AGG(tvo.owner, ', ') Owner
from tp_owner_base tvo
group by tvo.TenantId, tvo.TenantVendorId
)

select
tv.tenant_id,
tv.id,
tv.record_name Name,
tv.ContactEmail,
tv.Website,
tv.CreationDate,
tv.RiskRating,
tv.Criticality,
tv.Geography,
tv.Industry,
tpt.Tag,
tvo.Owner

from base tv
left join tp_tag tpt on tpt.TenantVendorId = tv.id
left join tp_owner tvo on tvo.TenantVendorId = tv.id;