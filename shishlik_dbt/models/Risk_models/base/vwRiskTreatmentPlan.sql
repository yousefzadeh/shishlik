{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            cast([TreatmentDescription] as nvarchar(4000)) TreatmentDescription,
            [TreatmentDate],
            cast([TreatmentName] as nvarchar(4000)) TreatmentName,
            [IsDeprecated],
            [Status],
            case
                when Status = 0 then 'New' when Status = 1 then 'Completed' when Status = 3 then 'In-Progress'
            end as StatusCode,
            -- ,case 
            -- when Status = 1 then 'Not Overdue'
            -- when getdate() > TreatmentDate then 'Overdue'
            -- else 'No DueDate'
            -- end as DueDateStatus
            case
                when Status in (1)
                then 'Not Overdue'
                when Status in (0, 3) and getdate() <= TreatmentDate
                then 'Not Overdue'
                when getdate() > TreatmentDate
                then 'Overdue'
                else 'No Due Date'
            end as DueDateStatus,
            TreatmentCompletedDate
        from {{ source("risk_models", "RiskTreatmentPlan") }} {{ system_remove_IsDeleted() }}
    )

select
    {{ col_rename("Id", "RiskTreatmentPlan") }},
    {{ col_rename("CreationTime", "RiskTreatmentPlan") }},
    {{ col_rename("CreatorUserId", "RiskTreatmentPlan") }},
    {{ col_rename("LastModificationTime", "RiskTreatmentPlan") }},

    {{ col_rename("LastModifierUserId", "RiskTreatmentPlan") }},
    {{ col_rename("IsDeleted", "RiskTreatmentPlan") }},
    {{ col_rename("DeleterUserId", "RiskTreatmentPlan") }},
    {{ col_rename("DeletionTime", "RiskTreatmentPlan") }},

    {{ col_rename("TenantId", "RiskTreatmentPlan") }},
    {{ col_rename("TreatmentDescription", "RiskTreatmentPlan") }},
    {{ col_rename("TreatmentDate", "RiskTreatmentPlan") }},

    {{ col_rename("TreatmentName", "RiskTreatmentPlan") }},
    {{ col_rename("IsDeprecated", "RiskTreatmentPlan") }},
    {{ col_rename("Status", "RiskTreatmentPlan") }},
    {{ col_rename("StatusCode", "RiskTreatmentPlan") }},
    {{ col_rename("DueDateStatus", "RiskTreatmentPlan") }},
    {{ col_rename("TreatmentCompletedDate", "RiskTreatmentPlan") }}
from base
