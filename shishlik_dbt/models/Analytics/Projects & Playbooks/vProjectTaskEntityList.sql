with proj_tsk as (
select
pt.TenantId,
pt.Project_Id,
pt.ProjectTask_Id,
pt.ProjectTask_Name
from {{ ref("vProjectTask") }} pt
)
, assess as (
select
tsa.TenantId, tsa.Task_Subtask_Id, string_agg(tsa.Task_Subtask_LinkedAssessment, '; ') Task_AssessmentList
from {{ ref("vTaskSubtaskAssessment") }} tsa
group by
tsa.TenantId, tsa.Task_Subtask_Id
)
, auth as (
select
tsa.TenantId, tsa.Task_Subtask_Id,
string_agg(tsa.Task_Subtask_LinkedAuthority, '; ') Task_AuthorityList
from {{ ref("vTaskSubtaskAuthority") }} tsa
group by
tsa.TenantId, tsa.Task_Subtask_Id
)
, auth_prov as (
select
tsa.TenantId, tsa.Task_Subtask_Id,
string_agg(tsa.Task_Subtask_LinkedAuthorityProvision, '; ') Task_AuthorityProvisionList
from {{ ref("vTaskSubtaskAuthorityProvision") }} tsa
group by
tsa.TenantId, tsa.Task_Subtask_Id
)
, ctrl_resp as (
select
tsc.TenantId, tsc.Task_Subtask_Id, string_agg(tsc.Task_SubTask_LinkedResponsibility, '; ') Task_ResponsibilityList
from {{ ref("vTaskSubtaskResponsibility") }} tsc
group by
tsc.TenantId, tsc.Task_Subtask_Id
)
, ctrl as (
select
tsc.TenantId, tsc.Task_Subtask_Id, string_agg(tsc.Task_Subtask_LinkedControl, '; ') Task_ControlList
from {{ ref("vTaskSubtaskControl") }} tsc
group by
tsc.TenantId, tsc.Task_Subtask_Id
)
, ctrl_set as (
select
tsc.TenantId, tsc.Task_Subtask_Id, string_agg(tsc.Task_Subtask_LinkedControlSet, '; ') Task_ControlSetList
from {{ ref("vTaskSubtaskControlSet") }} tsc
group by
tsc.TenantId, tsc.Task_Subtask_Id
)
, ris as (
select
tsr.TenantId, tsr.Task_Subtask_Id, string_agg(tsr.Task_Subtask_LinkedRisk, '; ') Task_RiskList
from {{ ref("vTaskSubtaskRisk") }} tsr
group by
tsr.TenantId, tsr.Task_Subtask_Id
)
, third_pty as (
select
tst.TenantId, tst.Task_Subtask_Id, string_agg(tst.Task_Subtask_LinkedThirdParty, '; ') Task_ThirdPartyList
from {{ ref("vTaskSubtaskThirdParty") }} tst
group by
tst.TenantId, tst.Task_Subtask_Id
)
, asset as (
select
tsa.TenantId, tsa.Task_Subtask_Id, string_agg(tsa.Task_Subtask_LinkedAsset, '; ') Task_AssetList
from {{ ref("vTaskSubtaskAsset") }} tsa
group by
tsa.TenantId, tsa.Task_Subtask_Id
)
, iss as (
select
tsi.TenantId, tsi.Task_Subtask_Id, string_agg(tsi.Task_Subtask_LinkedIssue, '; ') Task_IssueList
from {{ ref("vTaskSubtaskIssue") }} tsi
group by
tsi.TenantId, tsi.Task_Subtask_Id
)
, cus_regis as (
select
tsr.TenantId, tsr.Task_Subtask_Id, string_agg(tsr.Task_Subtask_LinkedRegister, '; ') Task_CustomRegisterList
from {{ ref("vTaskSubtaskRegister") }} tsr
group by
tsr.TenantId, tsr.Task_Subtask_Id
)
, cus_regis_rcrd as (
select
tsr.TenantId, tsr.Task_Subtask_Id, string_agg(tsr.Task_Subtask_LinkedRegisterRecord, '; ') Task_CustomRegisterRecordList
from {{ ref("vTaskSubtaskRegisterRecord") }} tsr
group by
tsr.TenantId, tsr.Task_Subtask_Id
)

select
pt.TenantId,
pt.Project_Id,
pt.ProjectTask_Id,
pt.ProjectTask_Name,
las.Task_AssessmentList,
au.Task_AuthorityList,
aup.Task_AuthorityProvisionList,
cr.Task_ResponsibilityList,
c.Task_ControlList,
cs.Task_ControlSetList,
r.Task_RiskList,
tp.Task_ThirdPartyList,
a.Task_AssetList,
i.Task_IssueList,
crr.Task_CustomRegisterList,
rr.Task_CustomRegisterRecordList

from proj_tsk pt
left join assess las on las.Task_Subtask_Id = pt.ProjectTask_Id
left join auth au on au.Task_Subtask_Id = pt.ProjectTask_Id
left join auth_prov aup on aup.Task_Subtask_Id = pt.ProjectTask_Id
left join ctrl_resp cr on cr.Task_Subtask_Id = pt.ProjectTask_Id
left join ctrl c on c.Task_Subtask_Id = pt.ProjectTask_Id
left join ctrl_set cs on cs.Task_Subtask_Id = pt.ProjectTask_Id
left join ris r on r.Task_Subtask_Id = pt.ProjectTask_Id
left join third_pty tp on tp.Task_Subtask_Id = pt.ProjectTask_Id
left join asset a on a.Task_Subtask_Id = pt.ProjectTask_Id
left join iss i on i.Task_Subtask_Id = pt.ProjectTask_Id
left join cus_regis crr on crr.Task_Subtask_Id = pt.ProjectTask_Id
left join cus_regis_rcrd rr on rr.Task_Subtask_Id = pt.ProjectTask_Id