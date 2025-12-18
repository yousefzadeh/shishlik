with ast as (
select
a.TenantId,
a.TenantName,
a.Asset_Id,
a.Asset_Name
from {{ ref("vAsset") }} a
)
, tags as (
select
atg.TenantId, atg.Asset_Id, string_agg(atg.Asset_Tag, '; ') Asset_TagList
from {{ ref("vAssetTags") }} atg
group by
atg.TenantId, atg.Asset_Id
)
, owners as (
select
ao.TenantId, ao.Asset_Id, string_agg(ao.Asset_OwnerName, '; ') Asset_OwnerList
from {{ ref("vAssetOwner") }} ao
group by
ao.TenantId, ao.Asset_Id
)
, acc_mem as (
select
am.TenantId, am.Asset_Id, string_agg(am.Asset_AccessMember, '; ') Asset_AccessMemberList
from {{ ref("vAssetAccessMembers") }} am
group by
am.TenantId, am.Asset_Id
)

select
a.TenantId,
a.TenantName,
a.Asset_Id,
a.Asset_Name,
t.Asset_TagList,
o.Asset_OwnerList,
am.Asset_AccessMemberList

from ast a
left join tags t on t.TenantId = a.TenantId and t.Asset_Id = a.Asset_Id
left join owners o on a.TenantId = a.TenantId and o.Asset_Id = a.Asset_Id
left join acc_mem am on am.TenantId = a.TenantId and am.Asset_Id = a.Asset_Id