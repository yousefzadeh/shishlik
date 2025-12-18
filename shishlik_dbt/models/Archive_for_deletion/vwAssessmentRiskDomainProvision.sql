{{ config(materialized="view") }}

select
    adpr.AssessmentDomainProvisionRisk_AssessmentDomainProvisionId,
    -- r.Risk_IsCurrent,
    STRING_AGG(r.Risk_Name, ',') as Assessment_Risk_Name
from {{ ref("vwAssessmentDomainProvisionRisk") }} adpr
inner join {{ ref("vwRisk") }} r on r.Risk_Id = adpr.AssessmentDomainProvisionRisk_RiskId
group by adpr.AssessmentDomainProvisionRisk_AssessmentDomainProvisionId--, r.Risk_IsCurrent
