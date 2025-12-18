{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [EditionId],
            [SubscriptionEndDateUtc],
            [IsInTrialPeriod],
            [SubscriptionPaymentType],
            [IsActive],
            [MarketplaceItemId],
            [SetForCancellation],
            cast([ExternalPlanId] as nvarchar(4000)) ExternalPlanId,
            cast([ExternalSubscriptionId] as nvarchar(4000)) ExternalSubscriptionId,
            [CancellingEditionId]
        from {{ source("tenant_models", "TenantEditions") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "TenantEditions") }},
    {{ col_rename("TenantId", "TenantEditions") }},
    {{ col_rename("EditionId", "TenantEditions") }},
    {{ col_rename("SubscriptionEndDateUtc", "TenantEditions") }},

    {{ col_rename("IsInTrialPeriod", "TenantEditions") }},
    {{ col_rename("SubscriptionPaymentType", "TenantEditions") }},
    {{ col_rename("IsActive", "TenantEditions") }},
    {{ col_rename("MarketplaceItemId", "TenantEditions") }},

    {{ col_rename("SetForCancellation", "TenantEditions") }},
    {{ col_rename("ExternalPlanId", "TenantEditions") }},
    {{ col_rename("ExternalSubscriptionId", "TenantEditions") }},
    {{ col_rename("CancellingEditionId", "TenantEditions") }}
from base
