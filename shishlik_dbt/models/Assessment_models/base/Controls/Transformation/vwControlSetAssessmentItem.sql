with
    qba_cs as (
        -- QBA Control Set Assessments
        select
            Assessment_Id,
            Assessment_TenantId,
            Assessment_AuthorityId,
            Assessment_PolicyId,
            Assessment_WorkflowId,
            Assessment_StatusCode
        from {{ ref("vwAssessment") }} ass
        where
            ass.Assessment_IsTemplate = 0
            and ass.Assessment_WorkflowId = 0
            and ass.Assessment_PolicyId is not NULL
    ),
    rba_cs as (
        -- RBA Control Set Assessments
        select
            Assessment_Id,
            Assessment_TenantId,
            Assessment_AuthorityId,
            Assessment_PolicyId,
            Assessment_WorkflowId,
            Assessment_StatusCode
        from {{ ref("vwAssessment") }} ass
        where
            ass.Assessment_IsTemplate = 0
            and ass.Assessment_WorkflowId = 1
            and ass.Assessment_PolicyId is not NULL
    ),
    -- -------------------------------------------------------
    csc as (
        -- Control Set - Control Hierarchy
        select pol.TenantId Tenant_Id, pol.Id ControlSet_Id, c.Id Control_Id
        from {{ source("assessment_models", "Policy") }} pol
        join
            {{ source("assessment_models", "PolicyDomain") }} pd on pd.PolicyId = pol.Id
        join {{ source("assessment_models", "Controls") }} c on c.PolicyDomainId = pd.Id
        where
            pol.TenantId = c.TenantId
            and pol.IsDeleted = 0
            and pd.IsDeleted = 0
            and c.IsDeleted = 0  -- check for accuracy of cascading delete
    ),
    -- -------------------------------------------------------
    qba_question_control as (
        select
            ad.AssessmentDomain_AssessmentId Assessment_Id,
            q.Question_Id,
            cq.ControlQuestion_ControlsId Control_Id,
            ad.AssessmentDomain_TenantId Tenant_Id
        from {{ ref("vwAssessmentDomain") }} ad
        join
            {{ ref("vwQuestion") }} q
            on q.Question_AssessmentDomainId = ad.AssessmentDomain_Id
        join
            {{ ref("vwControlQuestion") }} cq
            on cq.ControlQuestion_QuestionId = q.Question_Id
    ),
    rba_req_control as (
        select
            ad.AssessmentDomain_AssessmentId Assessment_Id,
            adc.AssessmentDomainControl_ControlsId Control_Id,
            ad.AssessmentDomain_TenantId Tenant_Id
        from {{ ref("vwAssessmentDomain") }} ad
        join
            {{ ref("vwAssessmentDomainControl") }} adc
            on adc.AssessmentDomainControl_AssessmentDomainId = ad.AssessmentDomain_Id
            and adc.AssessmentDomainControl_TenantId = ad.AssessmentDomain_TenantId
    ),
    -- ------------------------------------------------------
    csc_qba_cs as (
        select distinct
            csc.Tenant_Id,
            csc.ControlSet_Id,
            csc.Control_Id,
            'Question' ItemType,
            qqc.Assessment_ID,
            qqc.Question_Id Item_Id
        from csc
        inner join
            qba_question_control qqc
            on qqc.Control_Id = csc.Control_Id
            and qqc.Tenant_Id = csc.Tenant_Id
    ),
    csc_rba_cs as (
        select distinct
            csc.Tenant_Id,
            csc.ControlSet_Id,
            csc.Control_Id,
            'Control Requirement' ItemType,
            rrc.Assessment_ID,
            csc.Control_Id Item_Id
        from csc
        inner join
            rba_req_control rrc
            on rrc.Control_Id = csc.Control_Id
            and rrc.Tenant_Id = csc.Tenant_Id
    ),
    -- -----------------------------------------
    final as (
        select *
        from csc_qba_cs
        union all
        select *
        from csc_rba_cs
    )
select *
from final
