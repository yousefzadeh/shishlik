{{ config(materialized="view") }}

with
    base as (
        select

            adcrd. [Id],
            adcrd. [CreationTime],
            adcrd. [CreatorUserId],
            adcrd. [LastModificationTime],
            adcrd. [LastModifierUserId],
            adcrd. [IsDeleted],
            adcrd. [DeleterUserId],
            adcrd. [DeletionTime],
            adcrd. [TenantId],
            adcrd. [CustomFieldId],
            adcrd. [AssessmentDomainControlId],
            adcrd. [CustomFieldAttributeId],
            cast(cfa. [AttributeName] as nvarchar(4000)) AttributeName,
            cast(COALESCE(cfa. [AttributeName], adcrd. [CustomFieldText]) as nvarchar(4000)) CustomFieldResponseMax,
            cast((COALESCE(cfa. [AttributeName], adcrd. [CustomFieldText])) as varchar(100)) CustomFieldResponse,
            cast(adcrd. [CustomFieldText] as nvarchar(4000)) CustomFieldText,
            adcrd. [AssessmentResponseId]
        from {{ source("assessment_models", "AssessmentDomainControlResponseData") }} adcrd
        left join
            {{ source("assessment_models", "CustomFieldAttribute") }} cfa
            on cfa. [TenantId] = adcrd. [TenantId]
            and cfa. [Id] = adcrd. [CustomFieldAttributeId]
            and cfa. [IsDeleted] = 0

        where adcrd.IsDeleted = 0
    )
select
    {{ col_rename("ID", "AssessmentDomainControlResponseData") }},
    {{ col_rename("TenantId", "AssessmentDomainControlResponseData") }},
    {{ col_rename("CustomFieldId", "AssessmentDomainControlResponseData") }},
    {{ col_rename("AssessmentDomainControlId", "AssessmentDomainControlResponseData") }},

    {{ col_rename("CustomFieldAttributeId", "AssessmentDomainControlResponseData") }},
    {{ col_rename("AttributeName", "AssessmentDomainControlResponseData") }},
    {{ col_rename("CustomFieldResponseMax", "AssessmentDomainControlResponseData") }},
    {{ col_rename("CustomFieldResponse", "AssessmentDomainControlResponseData") }},
    {{ col_rename("CustomFieldText", "AssessmentDomainControlResponseData") }},
    {{ col_rename("AssessmentResponseId", "AssessmentDomainControlResponseData") }}

from base
