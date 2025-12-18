with base as (
select
r.Register_TenantId,
r.Register_Id,
r.Register_RegisterName
from {{ ref("vwRegister") }} r
)
, rgs_itm_own as (
select
b.Register_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(rro.IssueOwner_LastModificationTime, rro.IssueOwner_CreationTime) as Record_LastModificationTime,
'Owner' Module,
'Register Items' AssignedItemType,
rr.RegisterRecord_Name AssignedItemName,
'Registers'+ ' > ' + 'Register Items' AssignedItemParentType,
b.Register_RegisterName + ' > '+ rr.RegisterRecord_Name AssignedItemParentName
from base b
join {{ ref("vwRegisterRecord") }} rr
on rr.RegisterRecord_RegisterId = b.Register_Id
join {{ ref("vwIssueOwner") }} rro
on rro.IssueOwner_IssueId = rr.RegisterRecord_Id
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = rro.IssueOwner_UserId
)

, final as (
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
from rgs_itm_own
)

select * from final