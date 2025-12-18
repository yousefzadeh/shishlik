{{ config(materialized="view") }}

with
    base as (
        select
            -- system generated fields from app macro
            adprd. [Id],
            adprd. [CreationTime],
            adprd. [CreatorUserId],
            adprd. [LastModificationTime],
            adprd. [LastModifierUserId],
            adprd. [IsDeleted],
            adprd. [DeleterUserId],
            adprd. [DeletionTime],
            adprd. [TenantId],
            adprd. [CustomFieldId],
            adprd. [AssessmentDomainProvisionId],
            adprd. [CustomFieldAttributeId],
            cast(cfa. [AttributeName] as nvarchar(4000)) AttributeName,
            cast(adprd. [CustomFieldText] as nvarchar(4000)) CustomFieldText,
            cast(COALESCE(cfa. [AttributeName], adprd. [CustomFieldText]) as nvarchar(4000)) CustomFieldResponseMax,
            COALESCE(cfa. [AttributeName], adprd. [CustomFieldText]) CustomFieldResponse,
            adprd. [AssessmentResponseId],
            coalesce(adprd.LastModificationTime, adprd.CreationTime) UpdateTime
        from {{ source("assessment_models", "AssessmentDomainProvisionResponseData") }} adprd
        left join
            {{ source("assessment_models", "CustomFieldAttribute") }} cfa
            on cfa. [TenantId] = adprd. [TenantId]
            and cfa. [Id] = adprd. [CustomFieldAttributeId]
            and cfa. [IsDeleted] = 0

        where adprd.IsDeleted = 0
    )
select
    {{ col_rename("ID", "AssessmentDomainProvisionResponseData") }},
    {{ col_rename("TenantId", "AssessmentDomainProvisionResponseData") }},
    {{ col_rename("CustomFieldId", "AssessmentDomainProvisionResponseData") }},
    {{ col_rename("AssessmentDomainProvisionId", "AssessmentDomainProvisionResponseData") }},

    {{ col_rename("CustomFieldAttributeId", "AssessmentDomainProvisionResponseData") }},
    {{ col_rename("CustomFieldText", "AssessmentDomainProvisionResponseData") }},
    {{ col_rename("AttributeName", "AssessmentDomainProvisionResponseData") }},
    {{ col_rename("CustomFieldResponseMax", "AssessmentDomainProvisionResponseData") }},
    {{ col_rename("CustomFieldResponse", "AssessmentDomainProvisionResponseData") }},
    {{ col_rename("AssessmentResponseId", "AssessmentDomainProvisionResponseData") }},
    {{ col_rename("UpdateTime", "AssessmentDomainProvisionResponseData") }}
from base
