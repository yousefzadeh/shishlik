{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [QuestionId],
            [AssessmentId],
            [UserId],
            [TenantId],
            [OrganizationUnitId],
            CONCAT([QuestionId], [UserId], [TenantId], [OrganizationUnitId]) as PK,  /* Line item detail of the table*/
            cast(coalesce([LastModificationTime],[CreationTime]) as datetime2) as UpdateTime --date column addition for synapse incremental load
        from {{ source("assessment_models", "QuestionVendorUser") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("ID", "QuestionVendorUser") }},
    {{ col_rename("CreatorUserId", "QuestionVendorUser") }},
    {{ col_rename("UserId", "QuestionVendorUser") }},
    {{ col_rename("QuestionId", "QuestionVendorUser") }},
    {{ col_rename("AssessmentId", "QuestionVendorUser") }},
    {{ col_rename("TenantId", "QuestionVendorUser") }},
    {{ col_rename("OrganizationUnitId", "QuestionVendorUser") }},
    {{ col_rename("PK", "QuestionVendorUser") }},
    {{ col_rename("UpdateTime", "QuestionVendorUser") }}
from base
