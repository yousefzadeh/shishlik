with base as (
select
p.Policy_TenantId,
p.Policy_Id,
p.Policy_Name,
p.Policy_IsCurrent
from {{ ref("vwPolicy") }} p
)
, cs_own as (
select
b.Policy_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(ps.PolicyStakeHolders_LastModificationTime, ps.PolicyStakeHolders_CreationTime) as Record_LastModificationTime,
'Owner' Module,
'Control Set' AssignedItemType,
b.Policy_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
join {{ ref("vwPolicyStakeHolders") }} ps
on ps.PolicyStakeHolders_PolicyId = b.Policy_Id
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = ps.PolicyStakeHolders_UserId
where b.Policy_IsCurrent = 1 and ps.PolicyStakeHolders_Role = 1
)
, cs_rvw as (
select
b.Policy_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(ps.PolicyStakeHolders_LastModificationTime, ps.PolicyStakeHolders_CreationTime) as Record_LastModificationTime,
'Reviewer' Module,
'Control Set' AssignedItemType,
b.Policy_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
join {{ ref("vwPolicyStakeHolders") }} ps
on ps.PolicyStakeHolders_PolicyId = b.Policy_Id
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = ps.PolicyStakeHolders_UserId
where b.Policy_IsCurrent = 1 and ps.PolicyStakeHolders_Role = 2
)
, cs_rdr as (
select
b.Policy_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(ps.PolicyStakeHolders_LastModificationTime, ps.PolicyStakeHolders_CreationTime) as Record_LastModificationTime,
'Reader' Module,
'Control Set' AssignedItemType,
b.Policy_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
join {{ ref("vwPolicyStakeHolders") }} ps
on ps.PolicyStakeHolders_PolicyId = b.Policy_Id
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = ps.PolicyStakeHolders_UserId
where b.Policy_IsCurrent = 1 and ps.PolicyStakeHolders_Role = 3
)
, cs_app as (
select
b.Policy_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(ps.PolicyStakeHolders_LastModificationTime, ps.PolicyStakeHolders_CreationTime) as Record_LastModificationTime,
'Approver' Module,
'Control Set' AssignedItemType,
b.Policy_Name AssignedItemName,
'blank' AssignedItemParentType,
cast(NULL as varchar(128)) AssignedItemParentName
from base b
join {{ ref("vwPolicyStakeHolders") }} ps
on ps.PolicyStakeHolders_PolicyId = b.Policy_Id
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = ps.PolicyStakeHolders_UserId
where b.Policy_IsCurrent = 1 and ps.PolicyStakeHolders_Role = 4
)
, ctrl as (
select
c.Controls_TenantId,
c.Controls_Id,
c.Controls_Name,
c.Controls_IsCurrent,
c.Controls_PolicyDomainId
from {{ ref("vwControls") }} c
)
, ctrl_own as (
select
c.Controls_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(co.ControlOwner_LastModificationTime, co.ControlOwner_CreationTime) as Record_LastModificationTime,
'Owner' Module,
'Controls' AssignedItemType,
c.Controls_Name AssignedItemName,
'Control Set'+ ' > ' + 'Controls' AssignedItemParentType,
b.Policy_Name + ' > '+ c.Controls_Name AssignedItemParentName
from base b
join {{ ref("vwPolicyDomain") }} pd
on pd.PolicyDomain_PolicyId = b.Policy_Id
join ctrl c
on c.Controls_PolicyDomainId = pd.PolicyDomain_Id
join {{ ref("vwControlOwner") }} co
on co.ControlOwner_ControlId = c.Controls_Id
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = co.ControlOwner_UserId
where c.Controls_IsCurrent = 1
)
, resp_own as (
select
c.Controls_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(so.StatementOwner_LastModificationTime, so.StatementOwner_CreationTime) as Record_LastModificationTime,
'Owner' Module,
'Responsibilities' AssignedItemType,
s.Statement_Title AssignedItemName,
'Control Set'+ ' > ' + 'Controls' + ' > ' + 'Responsibilities' AssignedItemParentType,
b.Policy_Name + ' > '+ c.Controls_Name + ' > ' + s.Statement_Title AssignedItemParentName
from base b
join {{ ref("vwPolicyDomain") }} pd
on pd.PolicyDomain_PolicyId = b.Policy_Id
join ctrl c
on c.Controls_PolicyDomainId = pd.PolicyDomain_Id
join {{ ref("vwStatementControl") }} sc
on sc.StatementControl_ControlId = c.Controls_Id
join {{ ref("vwStatement") }} s
on s.Statement_Id = sc.StatementControl_StatementId
join {{ ref("vwStatementOwner") }} so
on so.StatementOwner_StatementId = s.Statement_Id
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = so.StatementOwner_UserId
where s.Statement_IsCurrent = 1
)
, resp_asg as (
select
c.Controls_TenantId TenantId,
au.AbpUsers_FullName UserName,
au.AbpUsers_EmailAddress UserEmail,
coalesce(sm.StatementMember_LastModificationTime, sm.StatementMember_CreationTime) as Record_LastModificationTime,
'Assignee' Module,
'Responsibilities' AssignedItemType,
s.Statement_Title AssignedItemName,
'Control Set'+ ' > ' + 'Controls' + ' > ' + 'Responsibilities' AssignedItemParentType,
b.Policy_Name + ' > '+ c.Controls_Name + ' > ' + s.Statement_Title AssignedItemParentName
from base b
join {{ ref("vwPolicyDomain") }} pd
on pd.PolicyDomain_PolicyId = b.Policy_Id
join ctrl c
on c.Controls_PolicyDomainId = pd.PolicyDomain_Id
join {{ ref("vwStatementControl") }} sc
on sc.StatementControl_ControlId = c.Controls_Id
join {{ ref("vwStatement") }} s
on s.Statement_Id = sc.StatementControl_StatementId
join {{ ref("vwStatementMember") }} sm
on sm.StatementMember_StatementId = s.Statement_Id
join {{ ref("vwAbpUser") }} au
on au.AbpUsers_Id = sm.StatementMember_UserId
where s.Statement_IsCurrent = 1
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
from cs_own

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
from cs_rvw

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
from cs_rdr

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
from cs_app

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
from ctrl_own

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
from resp_own

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
from resp_asg
)

select * from final