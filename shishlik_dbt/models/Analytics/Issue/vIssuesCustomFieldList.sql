with iss as (
select
i.TenantId,
i.TenantName,
i.Issues_Id,
i.Issues_Name
from {{ ref("vIssues") }} i
)
, iss_cus as (
select
icf.TenantId, icf.Issues_Id,
icf.Issues_CustomField + ' = "' + string_agg(icf.Issues_CustomFieldValue, '","') + '"' Issues_CustomFieldList,
icf.Issues_CustomField + ' = "' + string_agg(convert(varchar, icf.Issues_CustomFieldDateValue, 106), '","') + '"' Issues_CustomFieldDateList
from {{ ref("vIssuesCustomFields") }} icf
group by
icf.TenantId, icf.Issues_Id, icf.Issues_CustomField
)
, iss_cus_val as (
select 
TenantId, Issues_Id,
'[ ' + string_agg(Issues_CustomFieldList, '] [') + ' ]' Issues_CustomFieldValueList,
'[ ' + string_agg(Issues_CustomFieldDateList, '] [') + ' ]' Issues_CustomFieldDateValueList
from iss_cus
group by TenantId, Issues_Id
)

select
i.TenantId,
i.TenantName,
i.Issues_Id,
i.Issues_Name,
icv.Issues_CustomFieldValueList,
icv.Issues_CustomFieldDateValueList
from iss i
left join iss_cus_val icv on icv.Issues_Id = i.Issues_Id