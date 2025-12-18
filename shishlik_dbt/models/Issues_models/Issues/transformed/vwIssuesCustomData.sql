{{ config(materialized="view") }}

select 
icfv.Tenant_Id,
icfv.Issue_Id IssueFreeTextControlData_IssueId, 
icfv.CustomField_Name CustomLabel, 
cast(icfv.CustomField_Value as varchar(4000)) Value,
cast(dateadd(d, datediff(d, 0, CustomField_DateValue), 0) as datetime2) DateValue
from {{ ref("vwIssueCustomFieldValue") }} icfv
{# where Tenant_Id = 1384 #}