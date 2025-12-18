/*
Assessment Response Field Value Authority based union control based -
Assessment -> AssessmentDomain -> AssessmentDomainProvision -> AssessmentDomainProvisionResponseData -> CustomFieldId column tells you for which field response is added.
Assessment -> AssessmentDomain -> AssessmentDomainProvision -> AssessmentDomainProvisionResponseData -> CustomFieldAttributeId columns FK to CustomFieldAttributes table gives you selected response if Customfield is of type dropdown
Assessment -> AssessmentDomain -> AssessmentDomainProvision -> AssessmentDomainProvisionResponseData -> CustomFieldText columns gives you response value  if Customfield is of type short text or long text
UNION ALL
Assessment -> AssessmentDomain -> AssessmentDomainControl-> AssessmentDomainControlResponseData -> CustomFieldId column tells you for which field response is added.
Assessment -> AssessmentDomain -> AssessmentDomainControl-> AssessmentDomainControlResponseData -> CustomFieldAttributeId columns FK to CustomFieldAttributes table gives you selected response if Customfield is of type dropdown
Assessment -> AssessmentDomain -> AssessmentDomainControl-> AssessmentDomainControlResponseData -> CustomFieldText columns gives you response value  if Customfield is of type short text or long text
*/
with
    auth_based as (
        select
            AssessmentDomainProvision_AssessmentDomainId,
            'Provision' Requirement_Type,
            AssessmentDomainProvision_Id,
            AssessmentDomainProvision_TenantId,
            CustomField_Name,
            CustomField_DataTypeCode,
            case
                CustomField_DataType
                when 1  -- dropdown
                then CustomFieldAttribute_AttributeName
                when 2  -- short text
                then AssessmentDomainProvisionResponseData_CustomFieldText
                when 3  -- long text
                then AssessmentDomainProvisionResponseData_CustomFieldText
                else NULL
            end as ResponseValue
        from {{ ref("vwAssessmentDomainProvision") }} adp
        join
            {{ ref("vwAssessmentDomainProvisionResponseData") }} adprd
            on AssessmentDomainProvisionResponseData_AssessmentDomainProvisionId = AssessmentDomainProvision_Id
        join
            {{ ref("vwCustomFieldAttribute") }} cfa
            on CustomFieldAttribute_Id = AssessmentDomainProvisionResponseData_CustomFieldAttributeId
        join {{ ref("vwCustomField") }} cf on CustomField_Id = AssessmentDomainProvisionResponseData_CustomFieldId
    ),
    control_based as (
        select
            AssessmentDomainControl_AssessmentDomainId,
            'Control' Requirement_Type,
            AssessmentDomainControl_Id,
            AssessmentDomainControl_TenantId,
            CustomField_Name,
            CustomField_DataTypeCode,
            case
                CustomField_DataType
                when 1  -- dropdown
                then CustomFieldAttribute_AttributeName
                when 2  -- short text
                then AssessmentDomainControlResponseData_CustomFieldText
                when 3  -- long text
                then AssessmentDomainControlResponseData_CustomFieldText
                else NULL
            end as ResponseValue
        from {{ ref("vwAssessmentDomainControl") }} adc
        join
            {{ ref("vwAssessmentDomainControlResponseData") }} adcrd
            on AssessmentDomainControlResponseData_AssessmentDomainControlId = AssessmentDomainControl_Id
        join
            {{ ref("vwCustomFieldAttribute") }} cfa
            on CustomFieldAttribute_Id = AssessmentDomainControlResponseData_CustomFieldAttributeId
        join {{ ref("vwCustomField") }} cf on CustomField_Id = AssessmentDomainControlResponseData_CustomFieldId
    ),
    final as (
        select
            AssessmentDomainProvision_AssessmentDomainId Requirement_AssessmentDomainId,
            Requirement_Type,
            AssessmentDomainProvision_Id Requirement_Id,
            AssessmentDomainProvision_TenantId Requirement_TenantId,
            CustomField_Name Response_FieldName,
            CustomField_DataTypeCode Response_FieldType,
            ResponseValue Response_Value
        from auth_based
        union all
        select
            AssessmentDomainControl_AssessmentDomainId Requirement_AssessmentDomainId,
            Requirement_Type,
            AssessmentDomainControl_Id Requirement_Id,
            AssessmentDomainControl_TenantId Requirement_TenantId,
            CustomField_Name Response_FieldName,
            CustomField_DataTypeCode Response_FieldType,
            ResponseValue Response_Value
        from control_based
    )
select *
from final
