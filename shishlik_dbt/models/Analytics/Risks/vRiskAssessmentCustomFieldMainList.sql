with ris_assess as (
select
ra.TenantId,
ra.RiskAssessment_RiskId,
ra.RiskAssessment_Id,
ra.RiskAssessment_Name
from {{ ref("vRiskAssessment") }} ra
)
, ris_assess_cus as (
select
rcf.TenantId, rcf.RiskAssessment_Id,
rcf.CustomField + ' = "' + string_agg(rcf.CustomFieldValue, '","') + '"' RiskAssessment_CustomFieldList,
rcf.CustomField + ' = "' + string_agg(convert(varchar, rcf.CustomFieldDateValue, 106), '","') + '"' RiskAssessment_CustomFieldDateList
from {{ ref("vRiskAssessmentCustomFieldMain") }} rcf
group by
rcf.TenantId, rcf.RiskAssessment_Id, rcf.CustomField
)
, ris_cus_val as (
select 
TenantId, RiskAssessment_Id,
'[ ' + string_agg(RiskAssessment_CustomFieldList, '] [') + ' ]' RiskAssessment_CustomFieldValueList,
'[ ' + string_agg(RiskAssessment_CustomFieldDateList, '] [') + ' ]' RiskAssessment_CustomFieldDateValueList
from ris_assess_cus
group by TenantId, RiskAssessment_Id
)

select
ra.TenantId,
ra.RiskAssessment_Id,
ra.RiskAssessment_Name,
rcv.RiskAssessment_CustomFieldValueList,
rcv.RiskAssessment_CustomFieldDateValueList
from ris_assess ra
join ris_cus_val rcv on rcv.TenantId = ra.TenantId and rcv.RiskAssessment_Id = ra.RiskAssessment_Id