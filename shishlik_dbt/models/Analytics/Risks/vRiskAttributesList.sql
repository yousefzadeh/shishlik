with ris as (
select
r.TenantId,
r.TenantName,
r.Risk_Id,
r.Risk_Name
from {{ ref("vRisks") }} r
)
, tags as (
select
rt.TenantId, rt.Risk_Id, string_agg(rt.Risk_Tag, '; ') Risk_TagList
from {{ ref("vRiskTag") }} rt
group by
rt.TenantId, rt.Risk_Id
)
, owners as (
select
ro.TenantId, ro.Risk_Id, string_agg(ro.Risk_OwnerName, '; ') Risk_OwnerList
from {{ ref("vRiskOwners") }} ro
group by
ro.TenantId, ro.Risk_Id
)
, acc_mem as (
select
ram.TenantId, ram.Risk_Id, string_agg(ram.Risk_AccessMember, '; ') Risk_AccessMemberList
from {{ ref("vRiskAccessMembers") }} ram
group by
ram.TenantId, ram.Risk_Id
)

select
r.TenantId,
r.TenantName,
r.Risk_Id,
r.Risk_Name,
t.Risk_TagList,
o.Risk_OwnerList,
am.Risk_AccessMemberList

from ris r
left join tags t on t.Risk_Id = r.Risk_Id
left join owners o on o.Risk_Id = r.Risk_Id
left join acc_mem am on am.Risk_Id = r.Risk_Id