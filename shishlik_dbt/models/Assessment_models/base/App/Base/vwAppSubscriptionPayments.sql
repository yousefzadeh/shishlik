{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [Amount],
            [DayCount],
            [Gateway],
            cast([SuccessUrl] as nvarchar(4000)) SuccessUrl,
            [PaymentPeriodType],
            [Status],
            [TenantEditionId],
            cast([InvoiceNo] as nvarchar(4000)) InvoiceNo,
            cast([Description] as nvarchar(4000)) Description,
            cast([ErrorUrl] as nvarchar(4000)) ErrorUrl,
            [ExternalPaymentId],
            [IsRecurring],
            [TenantAddOnFeatureId],
            [SubTotalAmount],
            [Tax],
            [Quantity],
            [TenantUserSubscriptionId],
            [PaymentType]
        from {{ source("assessment_models", "AppSubscriptionPayments") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "AppSubscriptionPayments") }},
    {{ col_rename("Amount", "AppSubscriptionPayments") }},
    {{ col_rename("DayCount", "AppSubscriptionPayments") }},
    {{ col_rename("Gateway", "AppSubscriptionPayments") }},

    {{ col_rename("SuccessUrl", "AppSubscriptionPayments") }},
    {{ col_rename("PaymentPeriodType", "AppSubscriptionPayments") }},
    {{ col_rename("Status", "AppSubscriptionPayments") }},
    {{ col_rename("TenantEditionId", "AppSubscriptionPayments") }},

    {{ col_rename("InvoiceNo", "AppSubscriptionPayments") }},
    {{ col_rename("Description", "AppSubscriptionPayments") }},
    {{ col_rename("ErrorUrl", "AppSubscriptionPayments") }},
    {{ col_rename("ExternalPaymentId", "AppSubscriptionPayments") }},

    {{ col_rename("IsRecurring", "AppSubscriptionPayments") }},
    {{ col_rename("TenantAddOnFeatureId", "AppSubscriptionPayments") }},
    {{ col_rename("SubTotalAmount", "AppSubscriptionPayments") }},
    {{ col_rename("Tax", "AppSubscriptionPayments") }},

    {{ col_rename("Quantity", "AppSubscriptionPayments") }},
    {{ col_rename("TenantUserSubscriptionId", "AppSubscriptionPayments") }},
    {{ col_rename("PaymentType", "AppSubscriptionPayments") }}
from base
