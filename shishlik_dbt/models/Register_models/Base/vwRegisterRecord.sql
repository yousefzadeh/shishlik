{{ config(materialized="view") }}

with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            at2.AbpTenants_Name,
            cast([Name] as nvarchar(4000))[Name],
            cast([Description] as nvarchar(4000)) Description,
            [Status],
            case
                when [Status] = 1
                then 'Edit'
                when [Status] = 2
                then 'Published'
                when [Status] = 100
                then 'Deprecated'
                else 'Undefined'
            end as StatusCode,
            [ResolvedDate],
            [PublishedById],
            WorkflowStageId,
            cast([ReportedBy] as nvarchar(4000)) ReportedBy,
            case when wfs.WorkflowStage_Name is null then 'Unassigned'
			else wfs.WorkflowStage_Name end StageCode,
            [IsArchived],
            [RecordedDate],
            [DueDate],
            EntityRegisterItemId IdRef,
            [Priority],
            case
                when [Priority] = 1
                then '1 - Immediate'
                when [Priority] = 2
                then '2 - High'
                when [Priority] = 3
                then '3 - Medium'
                when [Priority] = 4
                then '4 - Low'
                else 'Not selected'
            end as PriorityCode,
            Coalesce(RecordedDate,CreationTime) ReportedTime,
            Coalesce(LastModificationTime,CreationTime) UpdatedTime,
            iss.EntityRegisterId RegisterId

        from {{ source("issue_models", "Issues") }} iss
        join {{ ref("vwEntityRegister") }} er on er.EntityRegister_Id = iss.EntityRegisterId
        left join {{ ref("vwWorkflowStage") }} wfs on wfs.WorkflowStage_Id = iss.WorkflowStageId
        join {{ ref("vwAbpTenants") }} at2 on at2.AbpTenants_Id = iss.TenantId
        {{ system_remove_IsDeleted() }}
        and iss.IsArchived = 0
        and iss.Status != 100
        and er.EntityRegister_EntityType = 4
    )

select
    {{ col_rename("Id", "RegisterRecord") }},
    {{ col_rename("TenantId", "RegisterRecord") }},
    {{ col_rename("Name", "RegisterRecord") }},
    {{ col_rename("Description", "RegisterRecord") }},
    {{ col_rename("Status", "RegisterRecord") }},
    {{ col_rename("StatusCode", "RegisterRecord") }},
    {{ col_rename("ResolvedDate", "RegisterRecord") }},
    {{ col_rename("PublishedById", "RegisterRecord") }},
    {{ col_rename("ReportedBy", "RegisterRecord") }},
    {{ col_rename("StageCode", "RegisterRecord") }},
    {{ col_rename("WorkflowStageId", "RegisterRecord") }},
    {{ col_rename("IsArchived", "RegisterRecord") }},
    {{ col_rename("RecordedDate", "RegisterRecord") }},
    {{ col_rename("CreationTime", "RegisterRecord") }},
    {{ col_rename("ReportedTime", "RegisterRecord") }},
    {{ col_rename("UpdatedTime", "RegisterRecord") }},
    {{ col_rename("DueDate", "RegisterRecord") }},
    {{ col_rename("IdRef", "RegisterRecord") }},
    {{ col_rename("Priority", "RegisterRecord") }},
    {{ col_rename("PriorityCode", "RegisterRecord") }},
    {{ col_rename("RegisterId", "RegisterRecord") }}
from base
