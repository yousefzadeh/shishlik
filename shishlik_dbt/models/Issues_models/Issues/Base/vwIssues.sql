{{ config(materialized="view") }}

with
    base as (
        select
            -- system generated fields from app macro
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
            end as StatusCode
            -- Edit (1)
            -- Published (2)
            -- Deprecated (100)
            ,
            [ParentIssueId],
            [ResolvedDate],
            datediff(day, CreationTime, ResolvedDate) days,
            [RootIssueId],
            [Version],
            [PublishedById],
            cast([ReportedBy] as nvarchar(4000)) ReportedBy,
            [Stage],
            case when wfs.WorkflowStage_Name is null then 'Unassigned'
			else wfs.WorkflowStage_Name end StageCode,
            WorkflowStageId,
            [IsArchived],
            [RecordedDate],
            [DueDate],
            IdRef,
            EntityRegisterItemId IdRef_New,
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
            end as PriorityCode
            -- Immediate (1)
            -- High (2)
            -- Medium (3)
            -- Low (4)
            ,
            Coalesce(RecordedDate,CreationTime) ReportedTime,
            Coalesce(LastModificationTime,CreationTime) UpdatedTime,
            case
                when Status = 2 and LEAD([CreationTime]) over (partition by COALESCE([RootIssueId], [Id]) order by [Version]) is NULL
                then 1
                else 0
            end LatestPublishedVersion
        from {{ source("issue_models", "Issues") }} iss
        join {{ ref("vwEntityRegister") }} er on er.EntityRegister_Id = iss.EntityRegisterId
        left join {{ ref("vwWorkflowStage") }} wfs on wfs.WorkflowStage_Id = iss.WorkflowStageId
        join {{ ref("vwAbpTenants") }} at2 on at2.AbpTenants_Id = iss.TenantId
        {{ system_remove_IsDeleted() }}
        and iss.IsArchived = 0
        and iss.Status != 100
        and er.EntityRegister_EntityType = 3
    )

select
    {{ col_rename("Id", "Issues") }},
    {{ col_rename("TenantId", "Issues") }},
    {{ col_rename("AbpTenants_Name", "Issues") }},
    {{ col_rename("Name", "Issues") }},
    {{ col_rename("Description", "Issues") }},
    {{ col_rename("Status", "Issues") }},
    {{ col_rename("StatusCode", "Issues") }},  -- derived   
    {{ col_rename("ParentIssueId", "Issues") }},
    {{ col_rename("ResolvedDate", "Issues") }},
    {{ col_rename("days", "Issues") }},
    {{ col_rename("RootIssueId", "Issues") }},
    {{ col_rename("Version", "Issues") }},
    {{ col_rename("PublishedById", "Issues") }},
    {{ col_rename("ReportedBy", "Issues") }},
    {{ col_rename("Stage", "Issues") }},
    {{ col_rename("StageCode", "Issues") }},  -- derived
    {{ col_rename("WorkflowStageId", "Issues") }},
    {{ col_rename("IsArchived", "Issues") }},
    {{ col_rename("RecordedDate", "Issues") }},
    {{ col_rename("CreationTime", "Issues") }},
    {{ col_rename("ReportedTime", "Issues") }}, -- derived
    {{ col_rename("UpdatedTime", "Issues") }}, -- derived
    {{ col_rename("DueDate", "Issues") }},
    {{ col_rename("IdRef", "Issues") }},
    {{ col_rename("IdRef_New", "Issues") }},
    {{ col_rename("Priority", "Issues") }},
    {{ col_rename("PriorityCode", "Issues") }},  -- derived
    {{ col_rename("LatestPublishedVersion", "Issues") }} -- derived
from base
{# and TenantId = 1384 #}

