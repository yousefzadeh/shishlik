{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Author] as nvarchar(4000)) Author,
            cast([CommonCause] as nvarchar(4000)) CommonCause,
            cast([Description] as nvarchar(4000)) Description,
            cast([Name] as nvarchar(4000))[Name],
            cast([PotentialImpact] as nvarchar(4000)) PotentialImpact,
            cast([Source] as nvarchar(4000)) Source,
            cast([GraphDbReferenceId] as nvarchar(4000)) GraphDbReferenceId
        from {{ source("assessment_models", "AbstractRisk") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AbstractRisk") }},
    {{ col_rename("TenantId", "AbstractRisk") }},
    {{ col_rename("Author", "AbstractRisk") }},
    {{ col_rename("CommonCause", "AbstractRisk") }},

    {{ col_rename("Description", "AbstractRisk") }},
    {{ col_rename("Name", "AbstractRisk") }},
    {{ col_rename("PotentialImpact", "AbstractRisk") }},

    {{ col_rename("Source", "AbstractRisk") }},
    {{ col_rename("GraphDbReferenceId", "AbstractRisk") }}
from base
