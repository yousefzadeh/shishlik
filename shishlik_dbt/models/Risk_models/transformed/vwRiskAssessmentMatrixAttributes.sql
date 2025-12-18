
{# 
DOC START
  - name: vwRiskAssessmentMatrixAttributes
    description: |
        * Multi valued attribute list of Risk Assessment
        * Risk Assessments +
        * Custom Attributes - Likelihood, Impact, Rating, Rating description
        * Risk Assessment Tags 

    columns:
      - name: MatrixName
        description: Name of the Matrix
      - name: RiskId
        description: Risk Id
      - name: TenantId
        description: Tenant Id for the login user for data access
      - name: RiskAssessmentId
        description: Risk Assessment Id - 0 if no assessment done for a risk
      - name: RiskAssessment_Label
        description: Risk Assessment Label - one label can have multiple assesssments
      - name: RiskAssessment_RiskLabelIsCurrent
        description: Latest Riaks Assessment that is associated with the Label
      - name: RiskAssessment_FavouriteId
        description: Favourite Risk Assessment Id - only one favourite assessment per risk
      - name: RiskAssessment_Title
        description: Risk Assessment Name
      - name: X_AxisLabel
        description: X Axis Label of the Risk Matrix
      - name: dynaX_AxisLabel
      - name: ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlId
      - name: x_attribute_Id
      - name: Likelihood
      - name: Likelihood_Label
      - name: Likelihood_Value
      - name: Y_AxisLabel
        description: Y Axis Label of the Risk Matrix
      - name: dynaY_AxisLabel
      - name: ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlId
      - name: y_attribute_Id
      - name: Impact
      - name: Impact_Label
      - name: Impact_Value
      - name: RiskAssessment_LatestFlag
        description: Flag to indicate if the assessment is latest or not - if no favourite assessment then latest assessment is flagged
      - name: Rating Rating
      - name: Rating_Label
      - name: Rating_Value
      - name: RiskAssessment_AssessmentDate
        description: Date when Assessment is created or updated
DOC END
#}

-- Fact table
with
    grain as (  -- one row per risk assessment that is not deleted
        select
            ra.RiskAssessment_RiskId as RiskId,
            ra.RiskAssessment_TenantId as TenantId,
            ra.RiskAssessment_Id as RiskAssessmentId,  -- 0 if No Assessment done for a Risk
            ra.RiskAssessment_Title as RiskAssessment_Title,
            ra.RiskAssessment_RiskLabelIsCurrent,
            ra.RiskAssessment_Label,
            ra.RiskAssessment_AssessmentDate as RiskAssessment_AssessmentDate,
            ra.RiskAssessment_FavouriteId, 
            ra.RiskAssessment_LatestFlag -- favorite assessment or latest assessment flag = 1
        from {{ ref("vwRiskAssessment") }} ra
        where ra.RiskAssessment_IsDeleted = 0
    ),
    -- Dimension Tables
    matrix_axis_relation as (
        select
            tpd.ThirdPartyDynamicFieldConfiguration_ThirdPartyControlId MatrixId,
            tpd.ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlId X_AxisLabelId,
            tpd.ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlId Y_AxisLabelId,
            ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlName dynaX_AxisLabelName,
            ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlName dynaY_AxisLabelName,
            x_tpc.ThirdPartyControl_Label X_AxisLabelName,
            ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlId,
            y_tpc.ThirdPartyControl_Label Y_AxisLabelName,
            ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlId
        from {{ ref("vwThirdPartyDynamicFieldConfiguration") }} tpd
        inner join
            {{ ref("vwThirdPartyControl") }} x_tpc
            on x_tpc.ThirdPartyControl_Id = tpd.ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlId
        inner join
            {{ ref("vwThirdPartyControl") }} y_tpc
            on y_tpc.ThirdPartyControl_Id = tpd.ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlId
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
            tpdfd.ThirdPartyDynamicFieldData_XAxisAttributeLabel as Likelihood,
            tpdfd.ThirdPartyDynamicFieldData_DynamicValue as Rating,
            tpdfd.ThirdPartyDynamicFieldData_XAxisAttributeId x_attribute_Id,
            tpdfd.ThirdPartyDynamicFieldData_YAxisAttributeId y_attribute_Id,
            tpa.ThirdPartyAttributes_Id,
            tpa.ThirdPartyAttributes_Label Likelihood_Label,
            cast(tpa.ThirdPartyAttributes_Value as int) Likelihood_Value,
            tpc.ThirdPartyControl_Id,
            tpdfd.ThirdPartyDynamicFieldData_Id
        from {{ ref("vwRiskAssessment") }} ra -- includes deleted assessments
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
            --Removed Tenant Id Join
        join
            {{ ref("vwThirdPartyDynamicFieldData") }} tpdfd
            on tpdfd.ThirdPartyDynamicFieldData_XAxisAttributeId = tpa.ThirdPartyAttributes_Id
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
            tpdfd.ThirdPartyDynamicFieldData_YAxisAttributeLabel as Impact,
            tpdfd.ThirdPartyDynamicFieldData_DynamicValue as Rating,
            tpdfd.ThirdPartyDynamicFieldData_XAxisAttributeId x_attribute_Id,
            tpdfd.ThirdPartyDynamicFieldData_YAxisAttributeId y_attribute_Id,
            tpa.ThirdPartyAttributes_Id,
            tpa.ThirdPartyAttributes_Label Impact_Label,
            cast(tpa.ThirdPartyAttributes_Value as int) Impact_Value,
            tpc.ThirdPartyControl_Id,
            tpdfd.ThirdPartyDynamicFieldData_Id
        from {{ ref("vwRiskAssessment") }} ra -- includes deleted assessments
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
            on tpdfd.ThirdPartyDynamicFieldData_YAxisAttributeId = tpa.ThirdPartyAttributes_Id
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
            cast(tpa.ThirdPartyAttributes_Value as int) as Rating_Value,
            tpa.ThirdPartyAttributes_Description as RatingDescription,
            racad.RiskAssessmentCustomAttributeData_Id,
            tpa.ThirdPartyAttributes_Id,
            tpc.ThirdPartyControl_Id
        from {{ ref("vwRiskAssessment") }} ra -- includes deleted assessments
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
            --Removed Tenant Id Join
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
    mar.dynaX_AxisLabelName dynaX_AxisLabel,
    mar.ThirdPartyDynamicFieldConfiguration_XAxisThirdPartyControlId,
    likelihood.x_attribute_Id,
    likelihood.Likelihood,
    likelihood.Likelihood_Label,
    likelihood.Likelihood_Value,
    mar.Y_AxisLabelName Y_AxisLabel,
    mar.dynaY_AxisLabelName dynaY_AxisLabel,
    mar.ThirdPartyDynamicFieldConfiguration_YAxisThirdPartyControlId,
    impact.y_attribute_Id,
    impact.Impact,
    impact.Impact_Label,
    impact.Impact_Value,
    rating.RiskAssessment_LatestFlag,
    rating.Rating Rating,
    rating.Rating_Label,
    rating.Rating_Value,
    grain.RiskAssessment_AssessmentDate
from grain -- only assessments that are not deleted
join
    rating on grain.RiskAssessmentId = rating.RiskAssessment_Id and grain.TenantId = rating.RiskAssessment_TenantId
join
    likelihood
    on grain.RiskAssessmentId = likelihood.RiskAssessment_Id
    and grain.TenantId = likelihood.RiskAssessment_TenantId
join
    impact
    on grain.RiskAssessmentId = impact.RiskAssessment_Id
    and grain.TenantId = impact.RiskAssessment_TenantId
    and likelihood.x_attribute_Id = impact.x_attribute_Id
    and likelihood.y_attribute_Id = impact.y_attribute_Id
join
    matrix_axis_relation mar
    on mar.MatrixId = rating.ThirdPartyControl_Id
    and likelihood.ThirdPartyControl_Id = mar.X_AxisLabelId
    and impact.ThirdPartyControl_Id = mar.Y_AxisLabelId
