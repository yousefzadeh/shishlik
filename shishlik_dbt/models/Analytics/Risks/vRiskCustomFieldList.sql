with ris as (
select
r.TenantId,
r.TenantName,
r.Risk_Id,
r.Risk_Name
from {{ ref("vRisks") }} r
)
, ris_cus as (
select
rcf.TenantId, rcf.Risk_Id,
rcf.Risk_CustomField + ' = "' + string_agg(rcf.Risk_CustomFieldValue, '","') + '"' Risk_CustomFieldList,
rcf.Risk_CustomField + ' = "' + string_agg(convert(varchar, rcf.Risk_CustomFieldDateValue, 106), '","') + '"' Risk_CustomFieldDateList
from {{ ref("vRiskCustomFields") }} rcf
group by
rcf.TenantId, rcf.Risk_Id, rcf.Risk_CustomField
)
, ris_cus_val as (
select 
TenantId, Risk_Id,
'[ ' + string_agg(Risk_CustomFieldList, '] [') + ' ]' Risk_CustomFieldValueList,
'[ ' + string_agg(Risk_CustomFieldDateList, '] [') + ' ]' Risk_CustomFieldDateValueList
from ris_cus
group by TenantId, Risk_Id
)

select
r.TenantId,
r.TenantName,
r.Risk_Id,
r.Risk_Name,
rcv.Risk_CustomFieldValueList,
rcv.Risk_CustomFieldDateValueList
from ris r
left join ris_cus_val rcv on rcv.Risk_Id = r.Risk_Id