{{ config(materialized="view") }}
with
    base as (
        select {{ system_fields_macro() }}, [ControlsId], [AuthorityReferenceId], [TenantId], [Similarity]
        from {{ source("assessment_models", "ProvisionControl") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ProvisionControl") }},
    {{ col_rename("ControlsId", "ProvisionControl") }},
    {{ col_rename("AuthorityReferenceId", "ProvisionControl") }},
    {{ col_rename("TenantId", "ProvisionControl") }},

    {{ col_rename("Similarity", "ProvisionControl") }}
from base
