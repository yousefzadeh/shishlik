{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [QuestionId],
            [AuthorityProvisionId],
            coalesce(LastModificationTime, CreationTime) UpdateTime
        from {{ source("assessment_models", "ProvisionQuestion") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ProvisionQuestion") }},
    {{ col_rename("TenantId", "ProvisionQuestion") }},
    {{ col_rename("QuestionId", "ProvisionQuestion") }},
    {{ col_rename("AuthorityProvisionId", "ProvisionQuestion") }},
    {{ col_rename("UpdateTime", "ProvisionQuestion") }}
from base
