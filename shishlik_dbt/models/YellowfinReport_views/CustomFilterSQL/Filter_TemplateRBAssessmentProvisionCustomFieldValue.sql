with 
    prov_custom as (
        SELECT 
        Tenant_Id,
        AuthorityProvisionCustomValue_AuthorityProvisionId,
        AuthorityProvisionCustomValue_FieldName,
        AuthorityProvisionCustomValue_Value
        from {{ ref("vwAuthorityProvisionCustomValue") }}
    ),
    ass as (
        select 
        Assessment_ID,
        Assessment_TenantId,
        Assessment_CreatedFromTemplateId,
        Assessment_Name
        from {{ ref("vwAssessment") }}
        where Assessment_IsTemplate = 0 -- Assessment
        AND Assessment_WorkFlowId = 1 -- RBA
    ),
    template as (
        select 
        Assessment_ID Template_Id,
        Assessment_TenantID Template_TenantId,
        Assessment_Name Template_Name,
        Assessment_WorkFlowId WorkflowId
        from {{ ref("vwAssessment") }}
        where Assessment_IsTemplate = 1 -- Template
        AND Assessment_WorkFlowId = 1 -- RBA
    ),
    prov_req as (
        select
        AssessmentDomainProvision_AssessmentDomainId,
        AssessmentDomainProvision_TenantId,
        AssessmentDomainProvision_AuthorityProvisionId
        from {{ ref("vwAssessmentDomainProvision") }}
    ),
    final as (
        SELECT DISTINCT
            template.Template_Name,
            template.WorkflowId,
            ass.Assessment_Name,
            prov_custom.AuthorityProvisionCustomValue_FieldName,
            prov_custom.AuthorityProvisionCustomValue_Value,
            ass.Assessment_TenantId 
        FROM {{ ref("vwAssessmentDomain") }} AS domain
            inner JOIN prov_req
            ON domain.AssessmentDomain_ID = prov_req.AssessmentDomainProvision_AssessmentDomainId
            and domain.AssessmentDomain_TenantID = prov_req.AssessmentDomainProvision_TenantId
        INNER JOIN ass
            ON ass.Assessment_ID = domain.AssessmentDomain_AssessmentId
            and ass.Assessment_TenantID = domain.AssessmentDomain_TenantId
        INNER JOIN template 
            ON template.Template_ID = ass.Assessment_CreatedFromTemplateId
            and  template.Template_TenantID = ass.Assessment_TenantId
        inner JOIN prov_custom
            ON prov_req.AssessmentDomainProvision_AuthorityProvisionId = prov_custom.AuthorityProvisionCustomValue_AuthorityProvisionId
            and prov_req.AssessmentDomainProvision_TenantId = prov_custom.Tenant_Id
    )
select 
Assessment_TenantId,
Template_Name,
Assessment_Name,
AuthorityProvisionCustomValue_FieldName,
AuthorityProvisionCustomValue_Value
from final
{# where Assessment_TenantId = 1384 #}
