{{ config(materialized="view") }}
with
    CustomField as (
        select
            [CustomField_ID],
            [CustomField_TenantId],
            [CustomField_Name],
            [CustomField_Description],
            [CustomField_DataType]
        from {{ ref("vwCustomField") }}
    ),
    CustomFieldAttribute as (
        select
            [CustomFieldAttribute_ID],
            [CustomFieldAttribute_TenantId],
            [CustomFieldAttribute_CustomFieldId],
            [CustomFieldAttribute_AttributeName]
        from {{ ref("vwCustomFieldAttribute") }}
    ),
    AssessmentCustomField as (
        select
            [AssessmentCustomField_ID],
            [AssessmentCustomField_TenantId],
            [AssessmentCustomField_CustomFieldId],
            [AssessmentCustomField_AssessmentId],
            [AssessmentCustomField_Order]
        from {{ ref("vwAssessmentCustomField") }}
    ),
    CustomFieldJoined as (
        select
            CF. [CustomField_ID],
            CF. [CustomField_TenantId],
            CF. [CustomField_Name],
            CF. [CustomField_Description],
            CF. [CustomField_DataType],
            CFA. [CustomFieldAttribute_ID],
            CFA. [CustomFieldAttribute_TenantId],
            CFA. [CustomFieldAttribute_CustomFieldId],
            CFA. [CustomFieldAttribute_AttributeName]
        from CustomField CF
        left join CustomFieldAttribute CFA on CF. [CustomField_ID] = CFA. [CustomFieldAttribute_ID]
    ),
    AssessmentCustomFieldJoined as (
        select
            ACF. [AssessmentCustomField_ID],
            ACF. [AssessmentCustomField_TenantId],
            ACF. [AssessmentCustomField_CustomFieldId],
            ACF. [AssessmentCustomField_AssessmentId],
            ACF. [AssessmentCustomField_Order],
            CFJ. [CustomField_ID],
            CFJ. [CustomField_TenantId],
            CFJ. [CustomField_Name],
            CFJ. [CustomField_Description],
            CFJ. [CustomField_DataType],
            CFJ. [CustomFieldAttribute_ID],
            CFJ. [CustomFieldAttribute_TenantId],
            CFJ. [CustomFieldAttribute_CustomFieldId],
            CFJ. [CustomFieldAttribute_AttributeName]
        from AssessmentCustomField ACF
        left join
            CustomFieldJoined CFJ
            on ACF. [AssessmentCustomField_CustomFieldId] = CFJ. [CustomFieldAttribute_CustomFieldId]
    )

select *
from AssessmentCustomFieldJoined
