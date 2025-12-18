SELECT DISTINCT
    at.Assessment_Name Template_Name,
    ass.Assessment_Name Assessment_Name,
    a.Authority_Name,
    ap.AuthorityProvision_Name
FROM
    {{ ref("vwAssessment") }} at
INNER JOIN
    {{ ref("vwAssessment") }} ass
    ON
        ass.Assessment_TenantId = at.Assessment_TenantId
        AND ass.Assessment_CreatedFromTemplateId = at.Assessment_ID
        AND ass.Assessment_IsTemplate = 0
        AND ass.Assessment_WorkFlowId = 1
INNER JOIN
    {{ ref("vwAssessmentDomain") }} ad
    ON
        ad.AssessmentDomain_TenantId = ass.Assessment_TenantId
        AND ad.AssessmentDomain_AssessmentId = ass.Assessment_ID
INNER JOIN
    {{ ref("vwAssessmentDomainProvision") }} adp
    ON
        adp.AssessmentDomainProvision_TenantId = ad.AssessmentDomain_TenantId
        AND adp.AssessmentDomainProvision_AssessmentDomainId = ad.AssessmentDomain_ID
INNER JOIN
    {{ ref("vwDirectAuthorityProvision") }} ap 
    ON 
        ap.Tenant_Id = adp.AssessmentDomainProvision_TenantId
        AND ap.AuthorityProvision_ID = adp.AssessmentDomainProvision_AuthorityProvisionId
INNER JOIN
    {{ ref("vwAuthority") }} a
    ON
        a.Authority_Id = ap.Authority_Id
WHERE
    at.Assessment_IsTemplate = 1
    AND at.Assessment_WorkFlowId = 1
    AND at.Assessment_TenantId IN (1384)