{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            cast([DisplayName] as nvarchar(4000)) DisplayName,
            cast([Name] as nvarchar(4000)) Name,
            cast([Discriminator] as nvarchar(4000)) Discriminator,
            [AnnualPrice],
            [ExpiringEditionId],
            [MonthlyPrice],
            [TrialDayCount],
            [WaitingDayAfterExpire],
            [MarketplaceItemId],
            [FixedPrice],
            [ServiceProviderId],
            [IsServiceProviderEdition],
            cast([DefaultExternalPlanId] as nvarchar(4000)) DefaultExternalPlanId,
            cast([ExternalProductId] as nvarchar(4000)) ExternalProductId,
            [IsUserBasedBaseEdition],
            [IsServiceProviderClientOnlyEdition],
            cast([ExternalPaymentGatewayAnnualAddOnId] as nvarchar(4000)) ExternalPaymentGatewayAnnualAddOnId,
            cast([ExternalPaymentGatewayAnnualPlanId] as nvarchar(4000)) ExternalPaymentGatewayAnnualPlanId,
            cast([ExternalPaymentGatewayMonthlyAddOnId] as nvarchar(4000)) ExternalPaymentGatewayMonthlyAddOnId,
            cast([ExternalPaymentGatewayMonthlyPlanId] as nvarchar(4000)) ExternalPaymentGatewayMonthlyPlanId,
            [MonthlyPriceForAnnualPlan]
        from {{ source("assessment_models", "AbpEditions") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AbpEditions") }},
    {{ col_rename("DisplayName", "AbpEditions") }},
    {{ col_rename("Name", "AbpEditions") }},
    {{ col_rename("Discriminator", "AbpEditions") }},

    {{ col_rename("AnnualPrice", "AbpEditions") }},
    {{ col_rename("ExpiringEditionId", "AbpEditions") }},
    {{ col_rename("MonthlyPrice", "AbpEditions") }},
    {{ col_rename("TrialDayCount", "AbpEditions") }},

    {{ col_rename("WaitingDayAfterExpire", "AbpEditions") }},
    {{ col_rename("MarketplaceItemId", "AbpEditions") }},
    {{ col_rename("FixedPrice", "AbpEditions") }},
    {{ col_rename("ServiceProviderId", "AbpEditions") }},

    {{ col_rename("IsServiceProviderEdition", "AbpEditions") }},
    {{ col_rename("DefaultExternalPlanId", "AbpEditions") }},
    {{ col_rename("ExternalProductId", "AbpEditions") }},
    {{ col_rename("IsUserBasedBaseEdition", "AbpEditions") }},

    {{ col_rename("IsServiceProviderClientOnlyEdition", "AbpEditions") }},
    {{ col_rename("ExternalPaymentGatewayAnnualAddOnId", "AbpEditions") }},
    {{ col_rename("ExternalPaymentGatewayAnnualPlanId", "AbpEditions") }},
    {{ col_rename("ExternalPaymentGatewayMonthlyAddOnId", "AbpEditions") }},

    {{ col_rename("ExternalPaymentGatewayMonthlyPlanId", "AbpEditions") }},
    {{ col_rename("MonthlyPriceForAnnualPlan", "AbpEditions") }}
from base
