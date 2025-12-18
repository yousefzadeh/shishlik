with custdd as (
select
icad.Uuid,
icad.Id,
icad.TenantId,
icad.IssueId,
tpc.Label CustomField,
tpa.label CustomFieldvalue,
NULL CustomFielddateValue
from {{ source("issue_ref_models", "IssueCustomAttributeData") }} icad
join {{ source("third-party_ref_models", "ThirdPartyAttributes") }} tpa
on tpa.Id = icad.ThirdPartyAttributesId and tpa.IsDeleted = 0
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = tpa.ThirdPartyControlId and tpc.IsDeleted = 0
where icad.IsDeleted = 0
)
, custxt as (
select
ic.Id,
ic.TenantId,
ic.IssueId,
tpc.Label CustomField,
ic.TextData CustomFieldValue,
NULL CustomFielddateValue
from {{ source("issue_ref_models", "IssueFreeTextControlData") }} ic
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = ic.ThirdPartyControlId and tpc.IsDeleted = 0
where ic.IsDeleted = 0
and ic.TextData is not null and ic.TextData != ''
)
, cusnum as (
select
ic.Id,
ic.TenantId,
ic.IssueId,
tpc.Label CustomField,
cast(ic.NumberValue as nvarchar(max)) CustomFieldValue,
NULL CustomFielddateValue
from {{ source("issue_ref_models", "IssueFreeTextControlData") }} ic
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = ic.ThirdPartyControlId and tpc.IsDeleted = 0
where ic.IsDeleted = 0
and ic.NumberValue is not null
)
, cusdate as (
select
ic.Id,
ic.TenantId,
ic.IssueId,
tpc.Label CustomField,
NULL CustomFieldValue,
ic.CustomDateValue CustomFielddateValue
from {{ source("issue_ref_models", "IssueFreeTextControlData") }} ic
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = ic.ThirdPartyControlId and tpc.IsDeleted = 0
where ic.IsDeleted = 0
and ic.CustomDateValue is not null
)

, final as (
select
TenantId,
IssueId,
CustomField,
CustomFieldValue,
CustomFielddateValue
from custdd

union all

select
TenantId,
IssueId,
CustomField,
CustomFieldValue,
CustomFielddateValue
from custxt

union all

select
TenantId,
IssueId,
CustomField,
CustomFieldValue,
CustomFielddateValue
from cusnum

union all

select
TenantId,
IssueId,
CustomField,
CustomFieldValue,
CustomFielddateValue
from cusdate
)

select 
TenantId,
IssueId Issues_Id,
CustomField Issues_CustomField,
CustomFieldValue Issues_CustomFieldValue,
CustomFielddateValue Issues_CustomFieldDateValue
from final