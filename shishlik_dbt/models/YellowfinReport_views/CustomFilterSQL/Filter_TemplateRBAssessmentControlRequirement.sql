SELECT DISTINCT
    at.Assessment_Name Template_Name,
    ass.Assessment_Name Assessment_Name,
    p.Policy_Name,
    c.Controls_Name
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
    {{ ref("vwAssessmentDomainControl") }} adc
    ON
        adc.AssessmentDomainControl_TenantId = ad.AssessmentDomain_TenantId
        AND adc.AssessmentDomainControl_AssessmentDomainId = ad.AssessmentDomain_ID
INNER JOIN
    {{ ref("vwControls") }} c
    ON 
        c.Controls_TenantId = adc.AssessmentDomainControl_TenantId
        AND c.Controls_Id = adc.AssessmentDomainControl_ControlsId
INNER JOIN
    {{ ref("vwPolicyDomain") }} pd
    ON
        pd.PolicyDomain_TenantId = c.Controls_TenantId
        AND pd.PolicyDomain_Id = c.Controls_PolicyDomainId
INNER JOIN
    {{ ref("vwPolicy") }} p
    ON
        pd.PolicyDomain_PolicyId = p.Policy_Id
        and pd.PolicyDomain_TenantId = p.Policy_TenantId
WHERE
    at.Assessment_IsTemplate = 1
    AND at.Assessment_WorkFlowId = 1
    AND at.Assessment_TenantId IN (1384)