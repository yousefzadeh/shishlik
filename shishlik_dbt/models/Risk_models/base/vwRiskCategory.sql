{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Name] as nvarchar(4000))[Name],
            cast([Description] as nvarchar(4000)) Description,
            cast([Color] as nvarchar(4000)) Color,
            cast([GraphDbReferenceId] as nvarchar(4000)) GraphDbReferenceId
        from {{ source("risk_models", "RiskCategory") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskCategory") }},
    {{ col_rename("CreationTime", "RiskCategory") }},
    {{ col_rename("CreatorUserId", "RiskCategory") }},
    {{ col_rename("LastModificationTime", "RiskCategory") }},

    {{ col_rename("LastModifierUserId", "RiskCategory") }},
    {{ col_rename("IsDeleted", "RiskCategory") }},
    {{ col_rename("DeleterUserId", "RiskCategory") }},
    {{ col_rename("DeletionTime", "RiskCategory") }},

    {{ col_rename("TenantId", "RiskCategory") }},
    {{ col_rename("Name", "RiskCategory") }},
    {{ col_rename("Description", "RiskCategory") }},
    {{ col_rename("Color", "RiskCategory") }},

    {{ col_rename("GraphDbReferenceId", "RiskCategory") }}
from base
