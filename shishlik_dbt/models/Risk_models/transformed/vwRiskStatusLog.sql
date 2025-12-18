-- Risk Status Log Details to fetch Change Time of Current Risk Status
with
    RiskStatusLog as (
        select distinct
            c.Id as [ChangeId],
            cast(c.EntityId as int) EntityId,
            c.EntityTypeFullName,
            cs.Id as [SetId],
            cs.ExtensionData,
            c.ChangeTime,
            cs.CreationTime,
            c.ChangeType,
            cs.UserId,
            cs.TenantId,
            cs.Reason,
            pc.Id as [PropChangeId],
            pc.PropertyName,
            -- , pc.OriginalValue
            -- , pc.NewValue
            ws1.WorkflowStage_Name OriginalValue,
            ws2.WorkflowStage_Name NewValue,
            case
                when lead(c.ChangeTime) over (partition by cs.TenantId, c.EntityId order by pc.Id) is null then 1 else 0
            end IsCurrent
        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ ref("vwWorkflowStage") }} ws1 on ws1.WorkflowStage_Id = pc.OriginalValue
        left join {{ ref("vwWorkflowStage") }} ws2 on ws2.WorkflowStage_Id = pc.NewValue
        where PropertyName = 'WorkflowStageId' and EntityTypeFullName = 'LegalRegTech.Risk.Risk'
    )

select
    [ChangeId],
    EntityId,
    EntityTypeFullName,
    [SetId],
    ExtensionData,
    ChangeTime,
    ChangeType,
    UserId,
    TenantId,
    Reason,
    [PropChangeId],
    PropertyName,
    OriginalValue,
    NewValue

from RiskStatusLog
where IsCurrent = 1
