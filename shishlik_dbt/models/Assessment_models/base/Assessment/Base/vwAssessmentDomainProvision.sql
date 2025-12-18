{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [AssessmentDomainId],
            [AuthorityProvisionId],
            [TenantId],
            coalesce(LastModificationTime, CreationTime) UpdateTime,
            [IsDocumentUploadMandatory]
        from {{ source("assessment_models", "AssessmentDomainProvision") }}
    {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AssessmentDomainProvision") }},
    {{ col_rename("AssessmentDomainId", "AssessmentDomainProvision") }},
    {{ col_rename("AuthorityProvisionId", "AssessmentDomainProvision") }},
    {{ col_rename("TenantId", "AssessmentDomainProvision") }},
    {{ col_rename("UpdateTime", "AssessmentDomainProvision") }},

    {{ col_rename("IsDocumentUploadMandatory", "AssessmentDomainProvision") }},
    {{ col_rename("IsDeleted", "AssessmentDomainProvision") }}
from base
