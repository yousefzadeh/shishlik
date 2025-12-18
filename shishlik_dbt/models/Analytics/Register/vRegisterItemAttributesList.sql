with ast as (
select
a.TenantId,
a.TenantName,
a.Record_Id RegisterItem_Id,
a.Record_Name RegisterItem_Name
from {{ ref("vRegisterRecord") }} a
)
, tags as (
select
atg.TenantId, atg.RegisterItem_Id, string_agg(atg.RegisterItem_Tag, '; ') RegisterItem_TagList
from {{ ref("vRegisterItemTags") }} atg
group by
atg.TenantId, atg.RegisterItem_Id
)
, owners as (
select
ao.TenantId, ao.RegisterItem_Id, string_agg(ao.RegisterItem_OwnerName, '; ') RegisterItem_OwnerList
from {{ ref("vRegisterItemOwner") }} ao
group by
ao.TenantId, ao.RegisterItem_Id
)
, acc_mem as (
select
am.TenantId, am.RegisterItem_Id, string_agg(am.RegisterItem_AccessMember, '; ') RegisterItem_AccessMemberList
from {{ ref("vRegisterItemAccessMember") }} am
group by
am.TenantId, am.RegisterItem_Id
)

select
a.TenantId,
a.TenantName,
a.RegisterItem_Id,
a.RegisterItem_Name,
t.RegisterItem_TagList,
o.RegisterItem_OwnerList,
am.RegisterItem_AccessMemberList

from ast a
left join tags t on t.TenantId = a.TenantId and t.RegisterItem_Id = a.RegisterItem_Id
left join owners o on a.TenantId = a.TenantId and o.RegisterItem_Id = a.RegisterItem_Id
left join acc_mem am on am.TenantId = a.TenantId and am.RegisterItem_Id = a.RegisterItem_Id