{{ config(materialized="view") }}
with
    base as (
        select
            {{ system_fields_macro() }},
            [TenantId],
            at2.AbpTenants_Name,
            cast([Name] as nvarchar(4000)) Title,
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
            iss.EntityRegisterId RegisterId,
            iss.ServiceNowId

        from {{ source("issue_models", "Issues") }} iss
        join {{ ref("vwEntityRegister") }} er on er.EntityRegister_Id = iss.EntityRegisterId
        left join {{ ref("vwWorkflowStage") }} wfs on wfs.WorkflowStage_Id = iss.WorkflowStageId
        join {{ ref("vwAbpTenants") }} at2 on at2.AbpTenants_Id = iss.TenantId
        {{ system_remove_IsDeleted() }}
        and iss.IsArchived = 0
        and iss.Status != 100
        and er.EntityRegister_EntityType = 5
    )

select
    {{ col_rename("Id", "Asset") }},
    {{ col_rename("TenantId", "Asset") }},
    {{ col_rename("Title", "Asset") }},
    {{ col_rename("Description", "Asset") }},
    {{ col_rename("Status", "Asset") }},
    {{ col_rename("StatusCode", "Asset") }},
    {{ col_rename("ResolvedDate", "Asset") }},
    {{ col_rename("PublishedById", "Asset") }},
    {{ col_rename("ReportedBy", "Asset") }},
    {{ col_rename("StageCode", "Asset") }},
    {{ col_rename("WorkflowStageId", "Asset") }},
    {{ col_rename("IsArchived", "Asset") }},
    {{ col_rename("RecordedDate", "Asset") }},
    {{ col_rename("CreationTime", "Asset") }},
    {{ col_rename("ReportedTime", "Asset") }},
    {{ col_rename("UpdatedTime", "Asset") }},
    {{ col_rename("DueDate", "Asset") }},
    {{ col_rename("IdRef", "Asset") }},
    {{ col_rename("Priority", "Asset") }},
    {{ col_rename("PriorityCode", "Asset") }},
    {{ col_rename("RegisterId", "Asset") }},
    {{ col_rename("ServiceNowId", "Asset") }}
from base