with main as (
    SELECT DISTINCT
        a.Assessment_TenantId Assessment_TenantId,
        at.Assessment_Name Template_Name,
        a.Assessment_Name_Responding_Team,
        cf.CustomField_Name,
        adprd.AssessmentDomainProvisionResponseData_CustomFieldResponse
    FROM
        {{ ref("vwAssessment") }} at
    INNER JOIN
        {{ ref("vwAssessment") }} a
        ON
            a.Assessment_TenantId = at.Assessment_TenantId
            AND a.Assessment_CreatedFromTemplateId = at.Assessment_ID
            AND a.Assessment_IsTemplate = 0 -- Assessment to Template
            AND a.Assessment_WorkFlowId = 1 -- RBA
    INNER JOIN
        {{ ref("vwAssessmentDomain") }} ad
        ON
            ad.AssessmentDomain_AssessmentId = a.Assessment_ID
            and ad.AssessmentDomain_TenantId = a.Assessment_TenantId
    INNER JOIN
        {{ ref("vwAssessmentDomainProvision") }} adp
        ON
            adp.AssessmentDomainProvision_AssessmentDomainId = ad.AssessmentDomain_ID
            and adp.AssessmentDomainProvision_TenantId = ad.AssessmentDomain_TenantId
    INNER JOIN
        {{ ref("vwAssessmentDomainProvisionResponseData") }} adprd
        ON
            adprd.AssessmentDomainProvisionResponseData_AssessmentDomainProvisionId = adp.AssessmentDomainProvision_ID
            and adprd.AssessmentDomainProvisionResponseData_TenantId = adp.AssessmentDomainProvision_TenantId
    INNER JOIN
        {{ ref("vwCustomField") }} cf
        ON
            cf.CustomField_ID = adprd.AssessmentDomainProvisionResponseData_CustomFieldId
            and cf.CustomField_TenantId = adprd.AssessmentDomainProvisionResponseData_TenantId
    WHERE
        at.Assessment_IsTemplate = 1
        AND at.Assessment_WorkFlowId = 1
),
null_query as (
    SELECT DISTINCT
        a.Assessment_TenantId Assessment_TenantId,
        at.Assessment_Name Template_Name,
        a.Assessment_Name_Responding_Team,
        cf.CustomField_Name,
    null as     AssessmentDomainProvisionResponseData_CustomFieldResponse
    FROM
        {{ ref("vwAssessment") }} at
    INNER JOIN
        {{ ref("vwAssessment") }} a
        ON
            a.Assessment_TenantId = at.Assessment_TenantId
            AND a.Assessment_CreatedFromTemplateId = at.Assessment_ID
            AND a.Assessment_IsTemplate = 0
            AND a.Assessment_WorkFlowId = 1
    INNER JOIN
        {{ ref("vwAssessmentDomain") }} ad
        ON
            ad.AssessmentDomain_AssessmentId = a.Assessment_ID
            and ad.AssessmentDomain_TenantId = a.Assessment_TenantId
    INNER JOIN
        {{ ref("vwAssessmentDomainProvision") }} adp
        ON
            adp.AssessmentDomainProvision_AssessmentDomainId = ad.AssessmentDomain_ID
            and adp.AssessmentDomainProvision_TenantId = ad.AssessmentDomain_TenantId
    left JOIN
        {{ ref("vwAssessmentCustomField") }} acf
        ON
            acf.AssessmentCustomField_AssessmentId = a.Assessment_ID
            and acf.AssessmentCustomField_TenantId = a.Assessment_TenantId
    left JOIN
        {{ ref("vwCustomField") }} cf
        ON
            cf.CustomField_ID = acf.AssessmentCustomField_CustomFieldId
            and cf.CustomField_TenantId = acf.AssessmentCustomField_TenantId
    WHERE
        at.Assessment_IsTemplate = 1
        AND at.Assessment_WorkFlowId = 1
),
final as (
    select * from main 
    union all
    select * from null_query
)
select 
    Assessment_TenantId,
    Template_Name,
    Assessment_Name_Responding_Team,
    CustomField_Name,
    AssessmentDomainProvisionResponseData_CustomFieldResponse
from final
{# where Assessment_TenantId in (1384) #}