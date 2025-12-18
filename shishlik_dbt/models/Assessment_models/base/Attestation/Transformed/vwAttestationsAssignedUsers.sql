with base as (
select
a.Attestations_TenantId,
a.Attestations_Id,
a.Attestations_Name
from {{ ref("vwAttestations") }} a
where a.Attestations_IsArchived = 0
)
, attes_own as (
select ao.AttestationOwners_TenantId, ao.AttestationOwners_AttestationId, au.AbpUsers_FullName, au.AbpUsers_EmailAddress, coalesce(ao.AttestationOwners_LastModificationTime, ao.AttestationOwners_CreationTime) as Record_LastModificationTime
from {{ ref("vwAttestationOwners") }} ao
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = ao.AttestationOwners_UserId
)
, attes_acm as (
select acm.AttestationAccessMember_TenantId, acm.AttestationAccessMember_AttestationId, au.AbpUsers_FullName, au.AbpUsers_EmailAddress, coalesce(acm.AttestationAccessMember_LastModificationTime, acm.AttestationAccessMember_CreationTime) as Record_LastModificationTime
from {{ ref("vwAttestationAccessMember") }} acm
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = acm.AttestationAccessMember_UserId
)
, attes_attstr as (
select acm.AttestationAttestors_TenantId, acm.AttestationAttestors_AttestationId, au.AbpUsers_FullName, au.AbpUsers_EmailAddress, coalesce(acm.AttestationAttestors_LastModificationTime, acm.AttestationAttestors_CreationTime) as Record_LastModificationTime
from {{ ref("vwAttestationAttestors") }} acm
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = acm.AttestationAttestors_UserId
)
, final as (
select
b.Attestations_TenantId TenantId,
ao.AbpUsers_FullName UserName,
ao.AbpUsers_EmailAddress UserEmail,
ao.Record_LastModificationTime,
'Owner' Module,
'Attestations' AssignedItemType,
b.Attestations_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
left join attes_own ao on ao.AttestationOwners_AttestationId = b.Attestations_Id
where ao.AbpUsers_FullName is not null

union all

select
b.Attestations_TenantId TenantId,
acm.AbpUsers_FullName UserName,
acm.AbpUsers_EmailAddress UserEmail,
acm.Record_LastModificationTime,
'Access Member' Module,
'Attestations' AssignedItemType,
b.Attestations_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
left join attes_acm acm on acm.AttestationAccessMember_AttestationId = b.Attestations_Id
where acm.AbpUsers_FullName is not null

union all

select
b.Attestations_TenantId TenantId,
atr.AbpUsers_FullName UserName,
atr.AbpUsers_EmailAddress UserEmail,
atr.Record_LastModificationTime,
'Attestor' Module,
'Attestations' AssignedItemType,
b.Attestations_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
left join attes_attstr atr on atr.AttestationAttestors_AttestationId = b.Attestations_Id
where atr.AbpUsers_FullName is not null
)

select
TenantId,
UserName,
UserEmail,
Record_LastModificationTime,
Module,
AssignedItemType,
AssignedItemName,
AssignedItemParentType,
AssignedItemParentName
from final