with custdd as (
select
rcad.Uuid,
rcad.Id,
rcad.TenantId,
rcad.RiskId,
tpc.Label CustomField,
tpa.label CustomFieldvalue,
NULL CustomFielddateValue
from {{ source("risk_ref_models", "RiskCustomAttributeData") }} rcad
join {{ source("third-party_ref_models", "ThirdPartyAttributes") }} tpa
on tpa.Id = rcad.ThirdPartyAttributesId and tpa.IsDeleted = 0
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = tpa.ThirdPartyControlId and tpc.IsDeleted = 0
where rcad.IsDeleted = 0
)
, users as (
select
rcad.Id,
rcad.TenantId,
rcad.RiskId,
tpc.Label CustomField,
au.Name+' '+au.Surname CustomFieldValue,
NULL CustomFielddateValue
from {{ source("risk_ref_models", "RiskCustomAttributeData") }} rcad
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = rcad.ThirdPartyControlId and tpc.IsDeleted = 0
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = rcad.UserId and au.IsDeleted = 0 and au.IsActive = 1
where rcad.IsDeleted = 0

union all

select
rcad.Id,
rcad.TenantId,
rcad.RiskId,
tpc.Label CustomField,
aou.DisplayName CustomFieldValue,
NULL CustomFielddateValue
from {{ source("risk_ref_models", "RiskCustomAttributeData") }} rcad
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = rcad.ThirdPartyControlId and tpc.IsDeleted = 0
join {{ source("abp_ref_models", "AbpOrganizationUnits") }} aou
on aou.Id = rcad.OrganizationUnitId and aou.IsDeleted = 0
where rcad.IsDeleted = 0
)
, custxt as (
select
rt.Id,
rt.TenantId,
rt.RiskId,
tpc.Label CustomField,
rt.TextData CustomFieldValue,
NULL CustomFielddateValue
from {{ source("risk_ref_models", "RiskThirdPartyControlCustomText") }} rt
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = rt.ThirdPartyControlId and tpc.IsDeleted = 0
where rt.IsDeleted = 0
and rt.TextData is not null
)
, cusnum as (
select
rt.Id,
rt.TenantId,
rt.RiskId,
tpc.Label CustomField,
cast(rt.NumberValue as nvarchar(max)) CustomFieldValue,
NULL CustomFielddateValue
from {{ source("risk_ref_models", "RiskThirdPartyControlCustomText") }} rt
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = rt.ThirdPartyControlId and tpc.IsDeleted = 0
where rt.IsDeleted = 0
and rt.NumberValue is not null
)
, cusdate as (
select
rt.Id,
rt.TenantId,
rt.RiskId,
tpc.Label CustomField,
NULL CustomFieldValue,
rt.CustomDateValue CustomFielddateValue
from {{ source("risk_ref_models", "RiskThirdPartyControlCustomText") }} rt
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = rt.ThirdPartyControlId and tpc.IsDeleted = 0
where rt.IsDeleted = 0
and rt.CustomDateValue is not null
)

, final as (
select
TenantId,
RiskId,
CustomField,
CustomFieldValue,
CustomFielddateValue
from custdd

union all

select
TenantId,
RiskId,
CustomField,
CustomFieldValue,
CustomFielddateValue
from users

union all

select
TenantId,
RiskId,
CustomField,
CustomFieldValue,
CustomFielddateValue
from custxt

union all

select
TenantId,
RiskId,
CustomField,
CustomFieldValue,
CustomFielddateValue
from cusnum

union all

select
TenantId,
RiskId,
CustomField,
CustomFieldValue,
CustomFielddateValue
from cusdate
)

select 
TenantId,
RiskId Risk_Id,
CustomField Risk_CustomField,
CustomFieldValue Risk_CustomFieldValue,
CustomFielddateValue Risk_CustomFieldDateValue
from final