{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            [EditionId],
            [IsActive],
            [SubscriptionEndDateUtc],
            [SubscriptionPaymentType],
            cast([ExternalSubscriptionId] as nvarchar(4000)) ExternalSubscriptionId,
            cast([ExternalPlanId] as nvarchar(4000)) ExternalPlanId,
            [Quantity]
        from {{ source("tenant_models", "TenantUserSubscription") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "TenantUserSubscription") }},
    {{ col_rename("TenantId", "TenantUserSubscription") }},
    {{ col_rename("EditionId", "TenantUserSubscription") }},
    {{ col_rename("IsActive", "TenantUserSubscription") }},

    {{ col_rename("SubscriptionEndDateUtc", "TenantUserSubscription") }},
    {{ col_rename("SubscriptionPaymentType", "TenantUserSubscription") }},
    {{ col_rename("ExternalSubscriptionId", "TenantUserSubscription") }},
    {{ col_rename("ExternalPlanId", "TenantUserSubscription") }},

    {{ col_rename("Quantity", "TenantUserSubscription") }}
from base
