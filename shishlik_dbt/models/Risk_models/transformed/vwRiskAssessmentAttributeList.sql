/*
* Multi valued attribute list of Risk Assessment
* Risk Assessments +
* Custom Attributes - Likelihood, Impact, Rating, Rating description
* Risk Assessment Tags*/
-- Fact table
with
    ratag as (
        -- RiskAssessmentTag_Tags
        -- One row per riskAssessmentId that has a tag
        select 
        rat.RiskAssessmentTag_TenantId, 
        rat.RiskAssessmentTag_RiskAssessmentId, 
        STRING_AGG(t.Tags_Name, ', ') as RiskAssessment_TagList
        from {{ ref("vwRiskAssessmentTag") }} rat
        join {{ ref("vwTags") }} as t on rat.RiskAssessmentTag_TagId = t.Tags_ID and rat.RiskAssessmentTag_TenantId = t.Tags_TenantId
        group by rat.RiskAssessmentTag_TenantId,rat.RiskAssessmentTag_RiskAssessmentId
    ),
    grain as (
        -- 1 row per risk assessment that is not deleted
        select
            ra.RiskId RiskAssessment_RiskId,
            ra.Id RiskAssessment_Id,
            ra.TenantId RiskAssessment_TenantId,
            ra.Title RiskAssessment_Title,
            ra.AssessmentDate RiskAssessment_AssessmentDate
        from {{ source("risk_models","RiskAssessment") }} ra
    ),
    -- Dimension Tables
    matrix_axis_relation as (
        select
            ThirdPartyDynamicFieldConfiguration_ThirdPartyControlId MatrixId,
            ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlId X_AxisLabelId,
            ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlId Y_AxisLabelId,
            vtpc.ThirdPartyControl_Label X_AxisLabelName,
            tpc.ThirdPartyControl_Label Y_AxisLabelName
        from {{ ref("vwThirdPartyDynamicFieldConfiguration") }} tpd
        left join
            {{ ref("vwThirdPartyControl") }} vtpc
            on vtpc.ThirdPartyControl_Id = tpd.ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlId
        left join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpc.ThirdPartyControl_Id = tpd.ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlId
    ),
    likelihood as (
        -- RiskAssessment_RiskAssessmentCustomAttributeData_ThirdPartyAttributes
        -- where ThirdPartyControl.EntityType = 4
        -- and ThirdPartyControl.Laabel = 'Likelihood'
        -- One row per RiskAssessment per Matrix Type per Likelihood Label
        select distinct
            racad.RiskAssessmentCustomAttributeData_RiskAssessmentId RiskAssessment_Id,
            racad.RiskAssessmentCustomAttributeData_TenantId RiskAssessment_TenantId,
            tpa.ThirdPartyAttributes_Label as Likelihood,
            tpdfd.ThirdPartyDynamicFieldData_XAxisAttributeId x_attribute_Id,
            tpdfd.ThirdPartyDynamicFieldData_YAxisAttributeId y_attribute_Id,
            tpc.ThirdPartyControl_Id
            {# 
            -- DEBUG
            -- tpc.ThirdPartyControl_Label,
            -- tpc.ThirdPartyControl_Type,
            -- tpc.ThirdPartyControl_TypeCode,
            -- tpc.ThirdPartyControl_Label,
            -- tpdfd.ThirdPartyDynamicFieldData_DynamicValue as Rating,
            -- tpa.ThirdPartyAttributes_Id,
            -- tpdfd.ThirdPartyDynamicFieldData_Id
            -#}
        from {{ ref("vwRiskAssessmentCustomAttributeData") }} racad
        join
            {{ ref("vwThirdPartyAttributes") }} tpa
            on racad.RiskAssessmentCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
        join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
            and tpc.ThirdPartyControl_EntityType = 4
            and tpc.ThirdPartyControl_Enabled = 1
            
        join
            {{ ref("vwThirdPartyDynamicFieldData") }} tpdfd
            on tpdfd.ThirdPartyDynamicFieldData_XAxisAttributeId = tpa.ThirdPartyAttributes_Id  
    ),
    impact as (
        -- RiskAssessment_RiskAssessmentCustomAttributeData_ThirdPartyAttributes
        -- where ThirdPartyControl.EntityType = 4
        -- and ThirdPartyControl.Label = 'Impact' ?
        -- One row per RiskAssessment per Matrix Type per Impact Label
        select distinct
            racad.RiskAssessmentCustomAttributeData_RiskAssessmentId RiskAssessment_Id,
            racad.RiskAssessmentCustomAttributeData_TenantId RiskAssessment_TenantId,
            tpa.ThirdPartyAttributes_Label as Impact,
            tpdfd.ThirdPartyDynamicFieldData_XAxisAttributeId x_attribute_Id,
            tpdfd.ThirdPartyDynamicFieldData_YAxisAttributeId y_attribute_Id,
            tpc.ThirdPartyControl_Id
            {# 
            -- DEBUG
            -- tpc.ThirdPartyControl_Label,
            -- tpc.ThirdPartyControl_Type,
            -- tpc.ThirdPartyControl_TypeCode,
            -- tpdfd.ThirdPartyDynamicFieldData_DynamicValue as Rating,
            -- tpa.ThirdPartyAttributes_Id
            -- tpdfd.ThirdPartyDynamicFieldData_Id
            -#}
        from {{ ref("vwRiskAssessmentCustomAttributeData") }} racad
        join
            {{ ref("vwThirdPartyAttributes") }} tpa
            on racad.RiskAssessmentCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
        join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
            and tpc.ThirdPartyControl_EntityType = 4
            and tpc.ThirdPartyControl_Enabled = 1
            
        join
            {{ ref("vwThirdPartyDynamicFieldData") }} tpdfd
            on tpdfd.ThirdPartyDynamicFieldData_YAxisAttributeId = tpa.ThirdPartyAttributes_Id  
    ),
    rating as (
        -- RiskAssessment_RiskAssessmentCustomAttributeData_ThirdPartyAttributes
        -- where ThirdPartyControl.EntityType = 4 - Risk Assessment
        -- and ThirdPartyControl.Label = 'Risk rating' - Type 2?
        -- One row per RiskAssessment per Matrix Type
        select
            tpc.ThirdPartyControl_Label MatrixName,
            racad.RiskAssessmentCustomAttributeData_RiskAssessmentId RiskAssessment_Id,
            racad.RiskAssessmentCustomAttributeData_TenantId RiskAssessment_TenantId,
            tpa.ThirdPartyAttributes_Name as Rating,
            tpa.ThirdPartyAttributes_Label as Rating_Label,
            tpa.ThirdPartyAttributes_Description as RatingDescription,
            tpc.ThirdPartyControl_Id
            {# 
            -- DEBUG
            -- tpc.ThirdPartyControl_Type,
            -- racad.RiskAssessmentCustomAttributeData_Id,
            -- tpa.ThirdPartyAttributes_Id 
            -#}
        from {{ ref("vwRiskAssessmentCustomAttributeData") }} racad
        join
            {{ ref("vwThirdPartyAttributes") }} tpa
            on racad.RiskAssessmentCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
        join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
            and tpc.ThirdPartyControl_EntityType = 4
            and tpc.ThirdPartyControl_Enabled = 1
            and tpc.ThirdPartyControl_Type = 2
            
    )
select distinct
    rating.MatrixName,
    grain.RiskAssessment_RiskId RiskId,
    grain.RiskAssessment_TenantId TenantId,
    grain.RiskAssessment_Id RiskAssessmentId,
    cast(grain.RiskAssessment_Title as nvarchar(4000)) RiskAssessment_Title,
    mar.X_AxisLabelName X_AxisLabel,
    likelihood.Likelihood,
    likelihood.x_attribute_Id,
    mar.Y_AxisLabelName Y_AxisLabel,
    impact.Impact,
    impact.y_attribute_Id,
    cast(rating.Rating as nvarchar(4000)) Rating,
    rating.Rating_Label,
    grain.RiskAssessment_AssessmentDate,
    cast(rating.RatingDescription as nvarchar(4000)) RatingDescription,
    ratag.RiskAssessment_TagList
{# 
    -- DEBUG
    -- likelihood.ThirdPartyControl_Id  X_AxisLabelId,
    -- impact.ThirdPartyControl_Id  Y_AxisLabelId,
    -- rating.ThirdPartyControl_Id  MatrixId,
    -- mar.Y_AxisLabelId mar_Y_AxisLabelId,
    -- mar.X_AxisLabelId mar_X_AxisLabelId
    -- DEBUG 
-#}
from grain
left join ratag 
    on grain.RiskAssessment_Id = ratag.RiskAssessmentTag_RiskAssessmentId 
    and grain.RiskAssessment_TenantId = ratag.RiskAssessmentTag_TenantId
inner join
    rating on grain.RiskAssessment_Id = rating.RiskAssessment_Id 
    and grain.RiskAssessment_TenantId = rating.RiskAssessment_TenantId
inner join
    likelihood
    on grain.RiskAssessment_Id = likelihood.RiskAssessment_Id
    and grain.RiskAssessment_TenantId = likelihood.RiskAssessment_TenantId
inner join
    impact
    on grain.RiskAssessment_Id = impact.RiskAssessment_Id
    and grain.RiskAssessment_TenantId = impact.RiskAssessment_TenantId
    and likelihood.x_attribute_Id = impact.x_attribute_Id
    and likelihood.y_attribute_Id = impact.y_attribute_Id
inner hash join
    matrix_axis_relation mar
    on mar.MatrixId = rating.ThirdPartyControl_Id
    and likelihood.ThirdPartyControl_Id = mar.X_AxisLabelId
    and impact.ThirdPartyControl_Id = mar.Y_AxisLabelId
-- where RiskAssessment_TenantId = 1384
-- and rating.MatrixName = 'Risk Matrix RST'
-- and grain.RiskAssessment_Id = 3490

