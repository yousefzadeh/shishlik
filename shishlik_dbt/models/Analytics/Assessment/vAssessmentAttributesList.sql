with assess as (
select
a.TenantId,
a.TenantName,
a.Assessment_Id,
a.Assessment_Name
from {{ ref("vAssessment") }} a
)
, tags as (
select
atg.TenantId, atg.Assessment_Id, string_agg(atg.Assessment_Tag, '; ') Assessment_TagList
from {{ ref("vAssessmentTag") }} atg
group by
atg.TenantId, atg.Assessment_Id
)
, owners as (
select
ao.TenantId, ao.Assessment_Id, string_agg(ao.Assessment_OwnerName, '; ') Assessment_OwnerList
from {{ ref("vAssessmentOwner") }} ao
group by
ao.TenantId, ao.Assessment_Id
)
, acc_mem as (
select
aam.TenantId, aam.Assessment_Id, string_agg(aam.Assessment_AccessMemberName, '; ') Assessment_AccessMemberList
from {{ ref("vAssessmentAccessMember") }} aam
group by
aam.TenantId, aam.Assessment_Id
)

select
a.TenantId,
a.TenantName,
a.Assessment_Id,
a.Assessment_Name,
t.Assessment_TagList,
o.Assessment_OwnerList,
am.Assessment_AccessMemberList

from assess a
left join tags t on t.Assessment_Id = a.Assessment_Id
left join owners o on o.Assessment_Id = a.Assessment_Id
left join acc_mem am on am.Assessment_Id = a.Assessment_Id