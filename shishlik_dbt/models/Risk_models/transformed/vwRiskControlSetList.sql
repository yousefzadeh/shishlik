with
    unique_controlset as (
        select rc.RiskControl_RiskId, p.Policy_Name
        from {{ ref("vwRiskControl") }} rc
        inner join {{ ref("vwControls") }} c on rc.RiskControl_ControlId = c.Controls_Id
        join {{ ref("vwPolicyDomain") }} pd on pd.PolicyDomain_Id = c.Controls_PolicyDomainId
        join {{ ref("vwPolicy") }} p on p.Policy_Id = pd.PolicyDomain_PolicyId and p.Policy_Status != 100
        group by rc.RiskControl_RiskId, p.Policy_Name
    ),
    controlset_list as (
        select
            cs.RiskControl_RiskId, left(STRING_AGG(cast(cs.Policy_Name as nvarchar(max)), ', '), 4000) as ControlSetList
        from unique_controlset cs
        group by cs.RiskControl_RiskId
    )
select *
from controlset_list
