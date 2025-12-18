with
    audit_log as (
        select
            ec.AbpEntityChanges_TenantId TenantId,
            ec.AbpEntityChanges_EntityTypeFullName TableName,  -- Table
            cast(ec.AbpEntityChanges_EntityId as int) TableId,  -- TableId
            case
                when ec.AbpEntityChanges_ChangeType = 0
                then 'Created'
                when ec.AbpEntityChanges_ChangeType = 1
                then 'Updated'
                else 'Others'
            end ChangeType,
            epc.AbpEntityPropertyChanges_PropertyName ColumnName,  -- Column
            epc.AbpEntityPropertyChanges_OriginalValue ColumnOriginalValue,  -- Original value
            epc.AbpEntityPropertyChanges_NewValue ColumnNewValue,  -- New value
            ec.AbpEntityChanges_ChangeTime UpdateTime  -- Updated time
        from {{ ref("vwAbpEntityChanges") }} as ec
        inner join
            {{ ref("vwAbpEntityPropertyChanges") }} as epc
            on ec.AbpEntityChanges_TenantId = epc.AbpEntityPropertyChanges_TenantId
            and ec.AbpEntityChanges_Id = epc.AbpEntityPropertyChanges_EntityChangeId
    ),
    risk_workflowstage as (
        select
            TenantId,
            'Risk' TableName,
            TableId RiskId,
            NULL RiskTreatmentId,
            ChangeType,
            ColumnName,
            original_wfs.Workflowstage_Name ColumnOriginalValue,
            new_wfs.Workflowstage_Name ColumnNewValue,
            UpdateTime
        from audit_log a
        left join
            {{ ref("vwWorkflowStage") }} original_wfs
            on a.ColumnOriginalValue = original_wfs.WorkflowStage_Id
            and a.TenantId = original_wfs.WorkflowStage_TenantId
        left join
            {{ ref("vwWorkflowStage") }} new_wfs
            on a.ColumnNewValue = new_wfs.WorkflowStage_Id
            and a.TenantId = new_wfs.WorkflowStage_TenantId
        where a.TableName = 'LegalRegTech.Risk.Risk' and a.ColumnName = 'WorkflowStageId'
    ),
    final as (select * from risk_workflowstage)
select *
from final
