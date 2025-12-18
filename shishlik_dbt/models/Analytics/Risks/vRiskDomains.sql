with risk_domain as (
select
rcad.TenantId,
rcad.RiskId, 
tpc.Label Risk_Domain,
tpa.Label Risk_DomainValue
from {{ source("risk_ref_models", "RiskCustomAttributeData") }} rcad
join {{ source("third-party_ref_models", "ThirdPartyAttributes") }} tpa
on tpa.Id = rcad.ThirdPartyAttributesId  and tpa.IsDeleted = 0
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = tpa.ThirdPartyControlId 
and tpc.Name = 'RiskDomain' and tpc.IsDeleted = 0
and tpc.Enabled = 1 and tpc.EntityType = 2
where rcad.IsDeleted = 0
)
, child_domain as (
select 
rcad.TenantId,
rcad.RiskId,
tpc.Label Risk_ChildDomain,
tpa.Label Risk_ChildDomainValue
from {{ source("risk_ref_models", "RiskCustomAttributeData") }} rcad
join {{ source("third-party_ref_models", "ThirdPartyAttributes") }} tpa
on tpa.Id = rcad.ThirdPartyAttributesId 
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = tpa.ThirdPartyControlId and tpc.Name is null and tpc.IsDeleted = 0
and tpc.Enabled = 1 and tpc.EntityType = 2
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc2 
on tpc2.Id = tpc.ParentThirdPartyControlId and tpc2.ParentThirdPartyControlId is null and tpc2.IsDeleted = 0
where rcad.IsDeleted =0
)
, grandchild_domain as (
select
rcad.TenantId,
rcad.RiskId,
tpc.Label Risk_GrandChildDomain,
tpa.Label Risk_GrandChildDomainValue
from {{ source("risk_ref_models", "RiskCustomAttributeData") }} rcad
join {{ source("third-party_ref_models", "ThirdPartyAttributes") }} tpa
on tpa.Id = rcad.ThirdPartyAttributesId and tpa.IsDeleted = 0
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc
on tpc.Id = tpa.ThirdPartyControlId and tpc.IsDeleted = 0
and tpc.Enabled = 1 and tpc.EntityType = 2
join {{ source("third-party_ref_models", "ThirdPartyControl") }} tpc2 
on tpc.ParentThirdPartyControlId = tpc2.Id and tpc2.IsDeleted = 0 and tpc2.Name is null
where rcad.IsDeleted =0
)

select
rd.TenantId,
rd.RiskId Risk_Id,
rd.Risk_Domain,
rd.Risk_DomainValue,
cd.Risk_ChildDomain,
cd.Risk_ChildDomainValue, 
gd.Risk_GrandChildDomain,
gd.Risk_GrandChildDomainValue
from risk_domain rd
left join child_domain cd on cd.RiskId = rd.RiskId
left join grandchild_domain gd on gd.RiskId = rd.RiskId