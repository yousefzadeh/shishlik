select
icad.Uuid,
icad.Id,
icad.TenantId,
icad.IssueId,
tpc.Label Issues_Field,
case when tpa.label is null then 'Not Selected' else tpa.label end Issues_Type
from {{ source("issue_ref_models", "IssueCustomAttributeData") }} icad
join {{ source("third-party_ref_models", "ThirdPartyAttributes") }} tpa
on tpa.Id = icad.ThirdPartyAttributesId and tpa.IsDeleted = 0
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = tpa.ThirdPartyControlId and tpc.IsDeleted = 0
where icad.IsDeleted = 0
and tpc.Label = 'Type'