-- controls linked to risks
with
    control_risk as (
        select distinct
            rc.RiskControl_TenantId Tenant_Id,
            rc.RiskControl_ControlId Controls_Id,
            r.Risk_Name
        from {{ ref("vwRiskControl") }} rc
        inner join {{ ref("vwRisk") }} r on rc.RiskControl_RiskId = r.Risk_Id
    ),
    final as (
        select Tenant_Id, Controls_Id, string_agg(Risk_Name, ', ') Risk_List
        from control_risk
        group by Tenant_Id, Controls_Id
    )
select *
from final
