{{ config(materialized="view") }}

with
    base as (
        select {{ system_fields_macro() }}, [TenantId], [RiskId], [Title], [SystemRiskAssessmentCode], [AssessmentDate]
        from {{ source("risk_models", "RiskAssessment") }}
    ),
    zero as (
        -- add zero key rows where no assessments are done for a risk for every Tenant
        select
            0 Id,
            r.CreationTime,
            r.CreatorUserId,
            r.LastModificationTime,

            r.LastModifierUserId,
            r.IsDeleted,
            r.DeleterUserId,
            r.DeletionTime,

            r.TenantId,
            r.Id RiskId,
            'No Risk Assessment' Title,
            NULL SystemRiskAssessmentCode,

            NULL AssessmentDate
        from {{ source("risk_models", "Risk") }} r
        left join base on r.Id = base.RiskId
        where base.RiskId is NULL

    ),
    ra as (
        -- for every riskid, a zero key is added where there are no assessments done
        select *
        from base
        union all
        select *
        from zero

    ),
    label as (
        select *
        from {{ ref("vwRiskAssessmentCustomAttributeData") }} racad
        join
            {{ ref("vwThirdPartyAttributes") }} tpa
            on racad.RiskAssessmentCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
        join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
            and tpc.ThirdPartyControl_EntityType = 4
            and tpc.ThirdPartyControl_Label = 'Risk Assessment Labels'
    ),
    ra_label as (
        select
            ra.RiskId RiskAssessment_RiskId,
            case
                when ral.RiskAssessmentCustomAttributeData_RiskAssessmentId is null
                then 'No label'
                when ra.Id = 0
                then ra.Title  -- no assessments
                else ral.ThirdPartyAttributes_Label
            end as RiskAssessment_Label,
            ra.AssessmentDate RiskAssessment_AssessmentDate,
            ra.Id RiskAssessment_Id
        from ra
        left join
            label ral
            on ra.Id = ral.RiskAssessmentCustomAttributeData_RiskAssessmentId
            and ra.TenantId = ral.RiskAssessmentCustomAttributeData_TenantId
    )
select
    RiskAssessment_RiskId as RiskAssessmentLabel_RiskId,
    cast(RiskAssessment_Label as nvarchar(4000)) RiskAssessmentLabel_Name,
    RiskAssessment_Id as RiskAssessmentLabel_RiskAssessmentId,
    ROW_NUMBER() over (
        partition by RiskAssessment_RiskId, RiskAssessment_Label order by RiskAssessment_AssessmentDate
    ) RiskAssessmentLabel_Version,
    case
        when
            ROW_NUMBER() over (
                partition by RiskAssessment_RiskId, RiskAssessment_Label order by RiskAssessment_AssessmentDate desc
            )
            = 1
        then 1
        else 0
    end RiskAssessmentLabel_IsCurrent
from ra_label
