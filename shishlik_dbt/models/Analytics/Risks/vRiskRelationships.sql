select
rr.TenantId,
rr.RiskId Risk_Id,
rr.RelationshipType Risk_RelationshipTypeId,
case 
when RelationshipType = 1 then 'Parent'
when RelationshipType = 2 then 'Child'
when RelationshipType = 3 then 'Related'
end Risk_RelationshipType,
rr.RelatedRiskId Risk_LinkedRiskId,
r.TenantEntityUniqueId Risk_LinkedRiskIdRef,
r.Name Risk_LinkedRiskName

from {{ source("risk_ref_models", "RiskRelationships") }} rr
join {{ source("risk_ref_models", "Risk") }} r
on r.Id = rr.RelatedRiskId and r.IsDeleted = 0
where rr.IsDeleted = 0