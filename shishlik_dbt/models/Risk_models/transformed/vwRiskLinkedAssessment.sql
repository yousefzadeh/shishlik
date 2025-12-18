select Risk_Id,
       Risk_TenantId, 
       LinkedAssessment
from (
SELECT DISTINCT
    r.Risk_Id,
    r.Risk_TenantId,
	a.Assessment_Name ProvisionAssessment,
	a2.Assessment_Name ControlAssessment,
	a3.Assessment_Name QBA_Assessment
FROM
	{{ ref("vwRisk") }} r 
LEFT JOIN 
	{{ ref("vwAssessmentRisk") }} AS ar3
	ON 
		r.Risk_Id = ar3.AssessmentRisk_RiskId 
LEFT JOIN 
	{{ ref("vwAssessment") }} AS a3 
	ON 
		ar3.AssessmentRisk_AssessmentId = a3.Assessment_ID 
left JOIN 
	{{ ref("vwAssessmentDomainProvisionRisk") }} AS ar
	ON 
		r.Risk_Id = ar.AssessmentDomainProvisionRisk_RiskId 
left JOIN 
	{{ ref("vwAssessment") }} AS a 
	ON 
		ar.AssessmentDomainProvisionRisk_AssessmentId = a.Assessment_ID 
left JOIN 
	{{ ref("vwAssessmentDomainControlRisk") }} AS ar2
	ON 
		r.Risk_Id = ar2.AssessmentDomainControlRisk_RiskId 
left JOIN 
	{{ ref("vwAssessment") }} AS a2
	ON 
		ar2.AssessmentDomainControlRisk_AssessmentId = a2.Assessment_ID
) s
unpivot (LinkedAssessment for col in (QBA_Assessment, ProvisionAssessment,ControlAssessment)) as t2