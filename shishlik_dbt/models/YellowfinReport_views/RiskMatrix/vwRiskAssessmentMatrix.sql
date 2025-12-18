/*
* Multi valued attribute list of Risk Assessment
* Risk Assessments +
* Custom Attributes - Likelihood, Impact, Rating, Rating description
* Risk Assessment Tags*/
-- Fact table
with
    ratag as (
        -- RiskAssessmentTag_Tags
        -- One row per riskAssessmentId
        select rat.RiskAssessmentTag_RiskAssessmentId, STRING_AGG(t.Tags_Name, ',') as RiskAssessmentTagList
        from {{ ref("vwRiskAssessmentTag") }} rat
        join {{ ref("vwTags") }} as t on rat.RiskAssessmentTag_TagId = t.Tags_ID
        group by rat.RiskAssessmentTag_RiskAssessmentId
    ),
    grain as (  -- one row per risk assessment
        select
            ra.RiskAssessment_RiskId as RiskId,
            ra.RiskAssessment_TenantId as TenantId,
            ra.RiskAssessment_Id as RiskAssessmentId,
            ra.RiskAssessment_Title as RiskAssessment_Title,
            ra.RiskAssessment_RiskLabelIsCurrent,
            ra.RiskAssessment_Label,
            ra.RiskAssessment_AssessmentDate as RiskAssessment_AssessmentDate,
            ra.RiskAssessment_FavouriteId,--exposed column
            cast(ratag.RiskAssessmentTagList as nvarchar(4000)) RiskAssessment_TagList
        from {{ ref("vwRiskAssessment") }} ra
        left join ratag on ra.RiskAssessment_Id = ratag.RiskAssessmentTag_RiskAssessmentId
        where ra.RiskAssessment_IsDeleted = 0
    ),
    -- Dimension Tables
    matrix_axis_relation as (
        select
            tpd.ThirdPartyDynamicFieldConfiguration_ThirdPartyControlId MatrixId,
            tpd.ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlId X_AxisLabelId,
            tpd.ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlId Y_AxisLabelId,
            -- ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlName X_AxisLabelName, 
            -- ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlName Y_AxisLabelName 
            vtpc.ThirdPartyControl_Label X_AxisLabelName,
            ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlId,
            tpc.ThirdPartyControl_Label Y_AxisLabelName,
            ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlId
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
            tpc.ThirdPartyControl_Label,
            tpc.ThirdPartyControl_Type,
            tpc.ThirdPartyControl_TypeCode,
            ra.RiskAssessment_RiskId,
            ra.RiskAssessment_Id,
            ra.RiskAssessment_TenantId,
            tpdfd.ThirdPartyDynamicFieldData_XAxisAttributeLabel,
            tpdfd.ThirdPartyDynamicFieldData_DynamicValue as Rating,
            tpdfd.ThirdPartyDynamicFieldData_XAxisAttributeId x_attribute_Id,
            tpdfd.ThirdPartyDynamicFieldData_YAxisAttributeId y_attribute_Id,
            tpa.ThirdPartyAttributes_Id,
            tpa.ThirdPartyAttributes_Label Likelihood,
            tpa.ThirdPartyAttributes_Value Likelihood_Value,
            tpc.ThirdPartyControl_Id,
            tpdfd.ThirdPartyDynamicFieldData_Id
        from {{ ref("vwRiskAssessment") }} ra
        join
            {{ ref("vwRiskAssessmentCustomAttributeData") }} racad
            on ra.RiskAssessment_Id = racad.RiskAssessmentCustomAttributeData_RiskAssessmentId
            and ra.RiskAssessment_TenantId = racad.RiskAssessmentCustomAttributeData_TenantId
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
            on tpdfd.ThirdPartyDynamicFieldData_XAxisAttributeId = tpa.ThirdPartyAttributes_Id  -- and tpc.ThirdPartyControl_Label = 'Likelihood'
    -- tpc.ThirdPartyControl_Id = ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlId
    ),
    impact as (
        -- RiskAssessment_RiskAssessmentCustomAttributeData_ThirdPartyAttributes
        -- where ThirdPartyControl.EntityType = 4
        -- and ThirdPartyControl.Laabel = 'Impact'
        -- One row per RiskAssessment per Matrix Type per Impact Label
        select distinct
            tpc.ThirdPartyControl_Label,
            tpc.ThirdPartyControl_Type,
            tpc.ThirdPartyControl_TypeCode,
            ra.RiskAssessment_RiskId,
            ra.RiskAssessment_Id,
            ra.RiskAssessment_TenantId,
            tpdfd.ThirdPartyDynamicFieldData_YAxisAttributeLabel,
            tpdfd.ThirdPartyDynamicFieldData_DynamicValue as Rating,
            tpdfd.ThirdPartyDynamicFieldData_XAxisAttributeId x_attribute_Id,
            tpdfd.ThirdPartyDynamicFieldData_YAxisAttributeId y_attribute_Id,
            tpa.ThirdPartyAttributes_Id,
            tpa.ThirdPartyAttributes_Label Impact,
            tpa.ThirdPartyAttributes_Value Impact_Value,
            tpc.ThirdPartyControl_Id,
            tpdfd.ThirdPartyDynamicFieldData_Id
        from {{ ref("vwRiskAssessment") }} ra
        join
            {{ ref("vwRiskAssessmentCustomAttributeData") }} racad
            on ra.RiskAssessment_Id = racad.RiskAssessmentCustomAttributeData_RiskAssessmentId
            and ra.RiskAssessment_TenantId = racad.RiskAssessmentCustomAttributeData_TenantId
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
            on tpdfd.ThirdPartyDynamicFieldData_YAxisAttributeId = tpa.ThirdPartyAttributes_Id  -- and tpc.ThirdPartyControl_Label = 'Impact'
    -- tpc.ThirdPartyControl_Id = ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlId
    ),
    rating as (
        -- RiskAssessment_RiskAssessmentCustomAttributeData_ThirdPartyAttributes
        -- where ThirdPartyControl.EntityType = 4
        -- and ThirdPartyControl.Laabel = 'Risk rating'
        -- One row per RiskAssessment per Matrix Type
        select
            tpc.ThirdPartyControl_Label MatrixName,
            tpc.ThirdPartyControl_Type,
            ra.RiskAssessment_RiskId,
            ra.RiskAssessment_Id,
            ra.RiskAssessment_TenantId,
            ra.RiskAssessment_LatestFlag,
            tpa.ThirdPartyAttributes_Name as Rating,
            tpa.ThirdPartyAttributes_Label as Rating_Label,
            tpa.ThirdPartyAttributes_Value as Rating_Value,
            tpa.ThirdPartyAttributes_Description as RatingDescription,
            racad.RiskAssessmentCustomAttributeData_Id,
            tpa.ThirdPartyAttributes_Id,
            tpc.ThirdPartyControl_Id
        from {{ ref("vwRiskAssessment") }} ra
        join
            {{ ref("vwRiskAssessmentCustomAttributeData") }} racad
            on ra.RiskAssessment_Id = racad.RiskAssessmentCustomAttributeData_RiskAssessmentId
            and ra.RiskAssessment_TenantId = racad.RiskAssessmentCustomAttributeData_TenantId
        join
            {{ ref("vwThirdPartyAttributes") }} tpa
            on racad.RiskAssessmentCustomAttributeData_ThirdPartyAttributesId = tpa.ThirdPartyAttributes_Id
        join
            {{ ref("vwThirdPartyControl") }} tpc
            on tpa.ThirdPartyAttributes_ThirdPartyControlId = tpc.ThirdPartyControl_Id
            and tpc.ThirdPartyControl_EntityType = 4
            and tpc.ThirdPartyControl_Enabled = 1
            and tpc.ThirdPartyControl_Type = 2
    -- and tpc.ThirdPartyControl_Label = 'Risk Rating'
    -- tpc.ThirdPartyControl_Id = ThirdPartyDynamicFieldConfiguration_ThirdPartyControlId
    )
select distinct
    rating.MatrixName,
    grain.RiskId,
    grain.TenantId,
    grain.RiskAssessmentId,
    grain.RiskAssessment_Label,
    grain.RiskAssessment_RiskLabelIsCurrent,
    grain.RiskAssessment_FavouriteId,
    cast(grain.RiskAssessment_Title as nvarchar(4000)) RiskAssessment_Title,
    mar.X_AxisLabelName X_AxisLabel,
    mar.ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlId,
    likelihood.x_attribute_Id,
    likelihood.Likelihood,
    likelihood.Likelihood_Value,
    mar.Y_AxisLabelName Y_AxisLabel,
    mar.ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlId,
    impact.y_attribute_Id,
    impact.Impact,
    impact.Impact_Value,
    -- rating.RiskAssessment_LatestFlag,
    rating.Rating,
    rating.Rating_Label,
    rating.Rating_Value,
    grain.RiskAssessment_AssessmentDate
-- cast(rating.RatingDescription as nvarchar(4000)) RatingDescription,
-- grain.RiskAssessment_TagList
-- DEBUG
-- likelihood.ThirdPartyControl_Id  X_AxisLabelId,
-- impact.ThirdPartyControl_Id  Y_AxisLabelId,
-- rating.ThirdPartyControl_Id  MatrixId,
-- mar.Y_AxisLabelId mar_Y_AxisLabelId,
-- mar.X_AxisLabelId mar_X_AxisLabelId
-- DEBUG
from grain
inner join
    rating on grain.RiskAssessmentId = rating.RiskAssessment_Id and grain.TenantId = rating.RiskAssessment_TenantId
inner join
    likelihood
    on grain.RiskAssessmentId = likelihood.RiskAssessment_Id
    and grain.TenantId = likelihood.RiskAssessment_TenantId
inner join
    impact
    on grain.RiskAssessmentId = impact.RiskAssessment_Id
    and grain.TenantId = impact.RiskAssessment_TenantId
    and likelihood.x_attribute_Id = impact.x_attribute_Id
    and likelihood.y_attribute_Id = impact.y_attribute_Id
inner join
    matrix_axis_relation mar
    on mar.MatrixId = rating.ThirdPartyControl_Id
    and likelihood.ThirdPartyControl_Id = mar.X_AxisLabelId
    and impact.ThirdPartyControl_Id = mar.Y_AxisLabelId

    -- where TenantId = 1384
    -- and rating.MatrixName = 'Risk Matrix RST'
    -- and grain.RiskAssessmentId = 3490
    
