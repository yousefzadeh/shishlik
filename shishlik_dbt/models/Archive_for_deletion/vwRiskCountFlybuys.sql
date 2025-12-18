with
    rst as (
        select distinct
            RISK.Risk_Name,
            RISK.Risk_TenantId,
            -- RISK.Risk_IsCurrent,
            RISK.Risk_IdRef,
            Risk_SnapshotDate as SnapshotDate,
            rcd.CustomLabel,
            rcd.Value,
            case when convert(date, RISK.Risk_CreationTime) = convert(date, RISK.Risk_SnapshotDate) then 1 end as C3,
            case when VWSTATUSLISTS_TreatmentDecision.StatusLists_Name = 'Accept' then 1 end as C4,
            case when VWWORKFLOWSTAGE.WorkflowStage_Name = 'Closed' then 1 end as C5,
            case when VWWORKFLOWSTAGE.WorkflowStage_Name != 'Closed' then 1 end as C6
        from {{ ref("vwRisk") }} as RISK
        inner join
            {{ ref("vwWorkflowStage") }} as VWWORKFLOWSTAGE
            on (RISK.Risk_WorkflowStageId = VWWORKFLOWSTAGE.WorkflowStage_Id)
        inner join
            {{ ref("vwRiskTreatment") }} as VWRISKTREATMENT on (RISK.Risk_Id = VWRISKTREATMENT.RiskTreatment_RiskId)
        left outer join
            {{ ref("vwStatusLists") }} as VWSTATUSLISTS_TreatmentDecision
            on (VWRISKTREATMENT.RiskTreatment_DecisionId = VWSTATUSLISTS_TreatmentDecision.StatusLists_Id)
        left outer join {{ ref("vwRisksCustomData") }} rcd on rcd.Risk_Id = RISK.Risk_Id

        union all

        select distinct
            RISK.Risk_Name,
            RISK.Risk_TenantId,
            -- RISK.Risk_IsCurrent,
            RISK.Risk_IdRef,
            Risk_CreationTime as SnapshotDate,
            rcd.CustomLabel,
            rcd.Value,
            1 as C3,
            case when VWSTATUSLISTS_TreatmentDecision.StatusLists_Name = 'Accept' then 1 end as C4,
            case when VWWORKFLOWSTAGE.WorkflowStage_Name = 'Closed' then 1 end as C5,
            case when VWWORKFLOWSTAGE.WorkflowStage_Name != 'Closed' then 1 end as C6
        from {{ ref("vwRisk") }} as RISK
        inner join
            {{ ref("vwWorkflowStage") }} as VWWORKFLOWSTAGE
            on (RISK.Risk_WorkflowStageId = VWWORKFLOWSTAGE.WorkflowStage_Id)
        inner join
            {{ ref("vwRiskTreatment") }} as VWRISKTREATMENT on (RISK.Risk_Id = VWRISKTREATMENT.RiskTreatment_RiskId)
        left outer join
            {{ ref("vwStatusLists") }} as VWSTATUSLISTS_TreatmentDecision
            on (VWRISKTREATMENT.RiskTreatment_DecisionId = VWSTATUSLISTS_TreatmentDecision.StatusLists_Id)
        left outer join {{ ref("vwRisksCustomData") }} rcd on rcd.Risk_Id = RISK.Risk_Id
    )

select
    rst.Risk_Name,
    rst.Risk_TenantId,
    -- rst.Risk_IsCurrent,
    rst.Risk_IdRef,
    rst.SnapshotDate,
    rst.CustomLabel,
    rst.Value,
    -- max(C3) AS Created,
    -- max(C4) AS Accept,
    -- max(C5) AS Completed,
    -- max(c6) AS OpenRisks
    C3 as Created,
    C4 as Accept,
    C5 as Completed,
    C6 as OpenRisks
from
    rst

    -- where rst.SnapshotDate BETWEEN CAST('20221106 00:00:00.000' as DATETIME) AND CAST('20221112 23:59:59.997' as
    -- DATETIME)
    -- AND rst.Risk_IsCurrent IN (1)
    -- and rst.Risk_TenantId = 1384
    -- group by
    -- rst.Risk_Name,
    -- rst.Risk_TenantId,
    -- rst.Risk_IsCurrent,
    -- rst.Risk_IdRef,
    -- rst.SnapshotDate,
    -- rst.CustomLabel,
    -- rst.Value
    
