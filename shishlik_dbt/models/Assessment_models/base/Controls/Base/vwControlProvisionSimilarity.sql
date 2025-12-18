{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [ControlId],
            [ProvisionId],
            [Match],
            cast([RowKeyInHailey] as nvarchar(4000)) RowKeyInHailey,
            [TenantId],
            [PolicyId]
        from {{ source("assessment_models", "ControlProvisionSimilarity") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "ControlProvisionSimilarity") }},
    {{ col_rename("ControlId", "ControlProvisionSimilarity") }},
    {{ col_rename("ProvisionId", "ControlProvisionSimilarity") }},
    {{ col_rename("Match", "ControlProvisionSimilarity") }},

    {{ col_rename("RowKeyInHailey", "ControlProvisionSimilarity") }},
    {{ col_rename("TenantId", "ControlProvisionSimilarity") }},
    {{ col_rename("PolicyId", "ControlProvisionSimilarity") }}
from base
