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
            epc.AbpEntityPropertyChanges_PropertyNameVarchar ColumnName,  -- Column
            epc.AbpEntityPropertyChanges_OriginalValue ColumnOriginalValue,  -- Original value
            epc.AbpEntityPropertyChanges_NewValue ColumnNewValue,  -- New value
            ec.AbpEntityChanges_ChangeTime UpdateTime  -- Updated time
        from {{ ref("vwAbpEntityChanges") }} as ec
        inner join
            {{ ref("vwAbpEntityPropertyChanges") }} as epc
            on ec.AbpEntityChanges_TenantId = epc.AbpEntityPropertyChanges_TenantId
            and ec.AbpEntityChanges_Id = epc.AbpEntityPropertyChanges_EntityChangeId
        where
            datediff(year, ec.AbpEntityChanges_ChangeTime, getdate()) <= 2
    ),
    final as (
        select
            a.TenantId,
            'RiskTreatmentPlan' TableName,
            rt.RiskTreatment_RiskId Risk_Id,
            rt.RiskTreatment_Id,
            a.TableId RiskTreatmentPlan_Id,
            a.ChangeType,
            a.ColumnName,
            a.ColumnOriginalValue,
            a.ColumnNewValue,
            a.UpdateTime
        from audit_log a
        join
            {{ ref("vwRiskTreatmentPlan") }} rtp
            on a.TenantId = rtp.RiskTreatmentPlan_TenantId
            and a.TableId = rtp.RiskTreatmentPlan_Id
        left join
            {{ ref("vwRiskTreatmentPlanAssociation") }} rtpa
            on rtp.RiskTreatmentPlan_Id = rtpa.RiskTreatmentPlanAssociation_RiskTreatmentPlanId
            and rtp.RiskTreatmentPlan_TenantId = rtpa.RiskTreatmentPlanAssociation_TenantId
        left join 
            {{ ref("vwRiskTreatment") }} rt
            on rtpa.RiskTreatmentPlanAssociation_RiskTreatmentId = rt.RiskTreatment_Id
        where
            a.TableName = 'LegalRegTech.Risk.RiskTreatmentPlan'
            and a.ColumnName = 'TreatmentDate'
            and ColumnOriginalValue is NULL
            and rt.RiskTreatment_RiskId is not NULL
    )
select 
TenantId,
Risk_Id,
RiskTreatment_Id,
RiskTreatmentPlan_Id,
cast(replace(ColumnNewValue,'"','') as datetime) OriginalDueDate
from final
