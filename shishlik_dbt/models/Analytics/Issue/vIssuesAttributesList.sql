with iss as (
select
i.TenantId,
i.TenantName,
i.Issues_Id,
i.Issues_Name
from {{ ref("vIssues") }} i
)
, tags as (
select
it.TenantId, it.Issues_Id, string_agg(it.Issues_Tag, '; ') Issues_TagList
from {{ ref("vIssuesTag") }} it
group by
it.TenantId, it.Issues_Id
)
, owners as (
select
io.TenantId, io.Issues_Id, string_agg(io.Issues_OwnerName, '; ') Issues_OwnerList
from {{ ref("vIssuesOwner") }} io
group by
io.TenantId, io.Issues_Id
)
, acc_mem as (
select
iam.TenantId, iam.Issues_Id, string_agg(iam.Issues_AccessMember, '; ') Issues_AccessMemberList
from {{ ref("vIssuesAccessMember") }} iam
group by
iam.TenantId, iam.Issues_Id
)

select
i.TenantId,
i.TenantName,
i.Issues_Id,
i.Issues_Name,
t.Issues_TagList,
o.Issues_OwnerList,
am.Issues_AccessMemberList

from iss i
left join tags t on t.Issues_Id = i.Issues_Id
left join owners o on o.Issues_Id = i.Issues_Id
left join acc_mem am on am.Issues_Id = i.Issues_Id