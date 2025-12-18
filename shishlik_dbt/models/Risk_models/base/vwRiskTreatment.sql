{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [RiskId],
            [TenantId],
            [Status],
            case
                when [Status] = 1
                then 'Draft'
                when [Status] = 2
                then 'Approved'
                when [Status] = 3
                then 'Treatment in progress'
                when [Status] = 4
                then 'Treatment paused'
                when [Status] = 5
                then 'Treatment cancelled'
                when [Status] = 6
                then 'Treatment completed'
                when [Status] = 7
                then 'Closed'
                else 'Undefined'
            end as [StatusCode],
            [DecisionId]
        from {{ source("risk_models", "RiskTreatment") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskTreatment") }},
    {{ col_rename("CreationTime", "RiskTreatment") }},
    {{ col_rename("CreatorUserId", "RiskTreatment") }},
    {{ col_rename("LastModificationTime", "RiskTreatment") }},

    {{ col_rename("LastModifierUserId", "RiskTreatment") }},
    {{ col_rename("IsDeleted", "RiskTreatment") }},
    {{ col_rename("DeleterUserId", "RiskTreatment") }},
    {{ col_rename("DeletionTime", "RiskTreatment") }},

    {{ col_rename("RiskId", "RiskTreatment") }},
    {{ col_rename("TenantId", "RiskTreatment") }},
    {{ col_rename("Status", "RiskTreatment") }},
    {{ col_rename("StatusCode", "RiskTreatment") }},
    {{ col_rename("DecisionId", "RiskTreatment") }}
from base
