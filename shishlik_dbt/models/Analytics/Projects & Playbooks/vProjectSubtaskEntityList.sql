with proj_subtsk as (
select
pst.TenantId,
pst.Project_Id,
pst.ProjectTask_Id,
pst.ProjectSubTask_Id,
pst.ProjectSubTask_Name
from {{ ref("vProjectSubTask") }} pst
)
, assess as (
select
tsa.TenantId, tsa.Task_Subtask_Id, string_agg(tsa.Task_Subtask_LinkedAssessment, '; ') Subtask_AssessmentList
from {{ ref("vTaskSubtaskAssessment") }} tsa
group by
tsa.TenantId, tsa.Task_Subtask_Id
)
, auth as (
select
tsa.TenantId, tsa.Task_Subtask_Id,
string_agg(tsa.Task_Subtask_LinkedAuthority, '; ') Subtask_AuthorityList
from {{ ref("vTaskSubtaskAuthority") }} tsa
group by
tsa.TenantId, tsa.Task_Subtask_Id
)
, auth_prov as (
select
tsa.TenantId, tsa.Task_Subtask_Id,
string_agg(tsa.Task_Subtask_LinkedAuthorityProvision, '; ') Subtask_AuthorityProvisionList
from {{ ref("vTaskSubtaskAuthorityProvision") }} tsa
group by
tsa.TenantId, tsa.Task_Subtask_Id
)
, ctrl_resp as (
select
tsc.TenantId, tsc.Task_Subtask_Id, string_agg(tsc.Task_SubTask_LinkedResponsibility, '; ') Subtask_ResponsibilityList
from {{ ref("vTaskSubtaskResponsibility") }} tsc
group by
tsc.TenantId, tsc.Task_Subtask_Id
)
, ctrl as (
select
tsc.TenantId, tsc.Task_Subtask_Id, string_agg(tsc.Task_Subtask_LinkedControl, '; ') Subtask_ControlList
from {{ ref("vTaskSubtaskControl") }} tsc
group by
tsc.TenantId, tsc.Task_Subtask_Id
)
, ctrl_set as (
select
tsc.TenantId, tsc.Task_Subtask_Id, string_agg(tsc.Task_Subtask_LinkedControlSet, '; ') Subtask_ControlSetList
from {{ ref("vTaskSubtaskControlSet") }} tsc
group by
tsc.TenantId, tsc.Task_Subtask_Id
)
, ris as (
select
tsr.TenantId, tsr.Task_Subtask_Id, string_agg(tsr.Task_Subtask_LinkedRisk, '; ') Subtask_RiskList
from {{ ref("vTaskSubtaskRisk") }} tsr
group by
tsr.TenantId, tsr.Task_Subtask_Id
)
, third_pty as (
select
tst.TenantId, tst.Task_Subtask_Id, string_agg(tst.Task_Subtask_LinkedThirdParty, '; ') Subtask_ThirdPartyList
from {{ ref("vTaskSubtaskThirdParty") }} tst
group by
tst.TenantId, tst.Task_Subtask_Id
)
, asset as (
select
tsa.TenantId, tsa.Task_Subtask_Id, string_agg(tsa.Task_Subtask_LinkedAsset, '; ') Subtask_AssetList
from {{ ref("vTaskSubtaskAsset") }} tsa
group by
tsa.TenantId, tsa.Task_Subtask_Id
)
, iss as (
select
tsi.TenantId, tsi.Task_Subtask_Id, string_agg(tsi.Task_Subtask_LinkedIssue, '; ') Subtask_IssueList
from {{ ref("vTaskSubtaskIssue") }} tsi
group by
tsi.TenantId, tsi.Task_Subtask_Id
)
, cus_regis as (
select
tsr.TenantId, tsr.Task_Subtask_Id, string_agg(tsr.Task_Subtask_LinkedRegister, '; ') Subtask_CustomRegisterList
from {{ ref("vTaskSubtaskRegister") }} tsr
group by
tsr.TenantId, tsr.Task_Subtask_Id
)
, cus_regis_rcrd as (
select
tsr.TenantId, tsr.Task_Subtask_Id, string_agg(tsr.Task_Subtask_LinkedRegisterRecord, '; ') Subtask_CustomRegisterRecordList
from {{ ref("vTaskSubtaskRegisterRecord") }} tsr
group by
tsr.TenantId, tsr.Task_Subtask_Id
)

select
pst.TenantId,
pst.Project_Id,
pst.ProjectTask_Id,
pst.ProjectSubTask_Id,
pst.ProjectSubTask_Name,
las.Subtask_AssessmentList,
au.Subtask_AuthorityList,
aup.Subtask_AuthorityProvisionList,
cr.Subtask_ResponsibilityList,
c.Subtask_ControlList,
cs.Subtask_ControlSetList,
r.Subtask_RiskList,
tp.Subtask_ThirdPartyList,
a.Subtask_AssetList,
i.Subtask_IssueList,
crr.Subtask_CustomRegisterList,
rr.Subtask_CustomRegisterRecordList

from proj_subtsk pst
left join assess las on las.Task_Subtask_Id = pst.ProjectSubTask_Id
left join auth au on au.Task_Subtask_Id = pst.ProjectSubTask_Id
left join auth_prov aup on aup.Task_Subtask_Id = pst.ProjectSubTask_Id
left join ctrl_resp cr on cr.Task_Subtask_Id = pst.ProjectSubTask_Id
left join ctrl c on c.Task_Subtask_Id = pst.ProjectSubTask_Id
left join ctrl_set cs on cs.Task_Subtask_Id = pst.ProjectSubTask_Id
left join ris r on r.Task_Subtask_Id = pst.ProjectSubTask_Id
left join third_pty tp on tp.Task_Subtask_Id = pst.ProjectSubTask_Id
left join asset a on a.Task_Subtask_Id = pst.ProjectSubTask_Id
left join iss i on i.Task_Subtask_Id = pst.ProjectSubTask_Id
left join cus_regis crr on crr.Task_Subtask_Id = pst.ProjectSubTask_Id
left join cus_regis_rcrd rr on rr.Task_Subtask_Id = pst.ProjectSubTask_Id