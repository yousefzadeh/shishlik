with base as (
select
i.Issues_TenantId,
i.Issues_Id,
i.Issues_Name
from {{ ref("vwIssues") }} i
where i.Issues_Status != 100
and i.Issues_IsArchived = 0
)
, iss_own as (
select io.IssueOwner_TenantId, io.IssueOwner_IssueId, au.AbpUsers_FullName, au.AbpUsers_EmailAddress, coalesce(io.IssueOwner_LastModificationTime, io.IssueOwner_CreationTime) as Record_LastModificationTime
from {{ ref("vwIssueOwner") }} io
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = io.IssueOwner_UserId
)
, iss_acm as (
select iu.IssueUser_TenantId, iu.IssueUser_IssueId, au.AbpUsers_FullName, au.AbpUsers_EmailAddress, coalesce(iu.IssueUser_LastModificationTime, iu.IssueUser_CreationTime) as Record_LastModificationTime
from {{ ref("vwIssueUser") }} iu
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = iu.IssueUser_UserId
)
, iss_users as (
select
b.Issues_TenantId TenantId,
io.AbpUsers_FullName UserName,
io.AbpUsers_EmailAddress UserEmail,
io.Record_LastModificationTime,
'Owner' Module,
'Issues' AssignedItemType,
b.Issues_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
left join iss_own io on io.IssueOwner_IssueId = b.Issues_Id
where io.AbpUsers_FullName is not null

union all

select
b.Issues_TenantId TenantId,
iu.AbpUsers_FullName UserName,
iu.AbpUsers_EmailAddress UserEmail,
iu.Record_LastModificationTime,
'Access Member' Module,
'Issues' AssignedItemType,
b.Issues_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
left join iss_acm iu on iu.IssueUser_IssueId = b.Issues_Id
where iu.AbpUsers_FullName is not null
)
, iss_act_asg as (
select
b.Issues_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(ia.IssueAction_LastModificationTime, ia.IssueAction_CreationTime) as Record_LastModificationTime,
'Assignee' Module,
'Issue Actions' AssignedItemType,
ia.IssueAction_Title AssignedItemName,
'Issues'+ ' > ' + 'Issue Actions' AssignedItemParentType,
b.Issues_Name+ ' > '+ ia.IssueAction_Title AssignedItemParentName
from base b
join {{ ref("vwIssueAction") }} ia
on ia.IssueAction_IssueId = b.Issues_Id
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = ia.IssueAction_UserId
)
, iss_form_own as (
select
isf.IssueSubmissionForm_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(isfo.IssueSubmissionFormOwner_LastModificationTime, isfo.IssueSubmissionFormOwner_CreationTime) as Record_LastModificationTime,
'Owner' Module,
'Issue SubmissionForm' AssignedItemType,
isf.IssueSubmissionForm_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from {{ ref("vwIssueSubmissionForm") }} isf
join {{ ref("vwIssueSubmissionFormOwner") }} isfo
on isfo.IssueSubmissionFormOwner_IssueSubmissionFormId = isf.IssueSubmissionForm_Id
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = isfo.IssueSubmissionFormOwner_UserId
where isf.IssueSubmissionForm_IsArchived = 0
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
from iss_users

union all

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
from iss_act_asg

union all

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
from iss_form_own
)

select * from final