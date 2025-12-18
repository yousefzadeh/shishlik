select 
adr.*,
Requirement_Type,
Requirement_Id,
Requirement_TenantId,
Requirement_ReferenceId,
Requirement_Name,
-- We need a unique key for the requirement, so we use the id and type to create one for distinct count
Requirement_Id * 2 + case when Requirement_Type = 'Provision' then 1 else 0 end Requirement_Key
from {{ ref("vwAssessmentDomainToResponse") }} adr
join {{ ref("vwRBARequirement") }} req on adr.AssessmentDomain_Id = req.AssessmentDomain_Id