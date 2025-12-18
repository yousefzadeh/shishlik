with
    audit_log as (
        select
            ec.AbpEntityChanges_TenantId TenantId,
            ec.AbpEntityChanges_EntityTypeFullName TableName,  -- Table
            cast(ec.AbpEntityChanges_EntityId as Int) TableId,  -- TableId
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
        where
            ec.AbpEntityChanges_EntityTypeFullName
            in ('LegalRegTech.Risk.Risk', 'LegalRegTech.Risk.RiskTreatment')
            and epc.AbpEntityPropertyChanges_PropertyName
            in ('WorkflowStageId', 'Status', 'DecisionId')
            and datediff(year, ec.AbpEntityChanges_ChangeTime, getdate()) <= 2
    ),
    risk_workflowstage as (
        select
            TenantId,
            'Risk' TableName,
            TableId Risk_Id,
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
        left join
            {{ ref("vwWorkflowStage") }} new_wfs
            on a.ColumnNewValue = new_wfs.WorkflowStage_Id
        where
            a.TableName = 'LegalRegTech.Risk.Risk' and a.ColumnName = 'WorkflowStageId'
    ),
    riskTreatment_completed as (
        select
            TenantId,
            'RiskTreatment' TableName,
            rt.RiskTreatment_RiskId Risk_Id,
            TableId RiskTreatment_Id,
            ChangeType,
            ColumnName,
            case
                ColumnOriginalValue
                when 1
                then 'Draft'
                when 2
                then 'Approved'
                when 3
                then 'Treatment in progress'
                when 4
                then 'Treatment paused'
                when 5
                then 'Treatment cancelled'
                when 6
                then 'Treatment completed'
                when 7
                then 'Closed'
            end as ColumnOriginalValue,
            case
                ColumnNewValue
                when 1
                then 'Draft'
                when 2
                then 'Approved'
                when 3
                then 'Treatment in progress'
                when 4
                then 'Treatment paused'
                when 5
                then 'Treatment cancelled'
                when 6
                then 'Treatment completed'
                when 7
                then 'Closed'
            end as ColumnNewValue,
            UpdateTime
        from audit_log a
        join
            {{ ref("vwRiskTreatment") }} rt
            on a.TenantId = rt.RiskTreatment_TenantId
            and a.TableId = rt.RiskTreatment_Id
        where
            a.TableName = 'LegalRegTech.Risk.RiskTreatment'
            and a.ColumnName = 'Status'
            and a.ColumnNewValue = 6  -- Completed
    ),
    riskTreatment_decision as (
        select
            TenantId,
            'RiskTreatment' TableName,
            rt.RiskTreatment_RiskId Risk_Id,
            TableId RiskTreatment_Id,
            ChangeType,
            ColumnName,
            original_sl.StatusLists_Name ColumnOriginalValue,
            new_sl.StatusLists_Name ColumnNewValue,
            UpdateTime
        from audit_log a
        join
            {{ ref("vwRiskTreatment") }} rt
            on a.TenantId = rt.RiskTreatment_TenantId
            and a.TableId = rt.RiskTreatment_Id
        left join
            {{ ref("vwStatusLists") }} original_sl
            on a.ColumnOriginalValue = original_sl.StatusLists_Id
        left join
            {{ ref("vwStatusLists") }} new_sl
            on a.ColumnNewValue = new_sl.StatusLists_Id
        where
            a.TableName = 'LegalRegTech.Risk.RiskTreatment'
            and a.ColumnName = 'DecisionId'
    ),
    uni as (
        select *
        from risk_workflowstage
        union all
        select *
        from risktreatment_completed
        union all
        select *
        from risktreatment_decision
    ),
    final as (
        select
            TenantId,
            TableName,
            Risk_Id,
            RiskTreatmentId,
            ChangeType,
            ColumnName,
            case
                when ColumnName = 'DecisionId'
                then 'Treatment Decision'
                when ColumnName = 'Status'
                then 'Treatment Status'
                when ColumnName = 'WorkflowStageId'
                then 'Workflow Stage'
            end as ColumnLabel,
            ColumnOriginalValue,
            ColumnNewValue,
            UpdateTime
        from uni
    )
select *
from final
