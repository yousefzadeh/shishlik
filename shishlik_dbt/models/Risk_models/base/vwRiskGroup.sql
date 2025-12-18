{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([Name] as nvarchar(4000))[Name],
            cast([Description] as nvarchar(4000)) Description,
            [Status],
            cast([ImageUrl] as nvarchar(4000)) ImageUrl,
            cast([GraphDbReferenceId] as nvarchar(4000)) GraphDbReferenceId,
            cast([Color] as nvarchar(4000)) Color,
            [IsAdRiskGroup],
            [NumberOfRisks],
            [Price],
            cast([WebLink] as nvarchar(4000)) WebLink,
            [SubscriptionType],
            [IsInternal]
        from {{ source("risk_models", "RiskGroup") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskGroup") }},
    {{ col_rename("CreationTime", "RiskGroup") }},
    {{ col_rename("CreatorUserId", "RiskGroup") }},
    {{ col_rename("LastModificationTime", "RiskGroup") }},

    {{ col_rename("LastModifierUserId", "RiskGroup") }},
    {{ col_rename("IsDeleted", "RiskGroup") }},
    {{ col_rename("DeleterUserId", "RiskGroup") }},
    {{ col_rename("DeletionTime", "RiskGroup") }},

    {{ col_rename("TenantId", "RiskGroup") }},
    {{ col_rename("Name", "RiskGroup") }},
    {{ col_rename("Description", "RiskGroup") }},
    {{ col_rename("Status", "RiskGroup") }},

    {{ col_rename("ImageUrl", "RiskGroup") }},
    {{ col_rename("GraphDbReferenceId", "RiskGroup") }},
    {{ col_rename("Color", "RiskGroup") }},
    {{ col_rename("IsAdRiskGroup", "RiskGroup") }},

    {{ col_rename("NumberOfRisks", "RiskGroup") }},
    {{ col_rename("Price", "RiskGroup") }},
    {{ col_rename("WebLink", "RiskGroup") }},
    {{ col_rename("SubscriptionType", "RiskGroup") }},

    {{ col_rename("IsInternal", "RiskGroup") }}
from base
