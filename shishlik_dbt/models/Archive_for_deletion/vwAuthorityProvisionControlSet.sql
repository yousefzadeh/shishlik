{{ config(materialized="view") }}

select
    c.Controls_TenantId,
    c.Controls_Id,
    pd.PolicyDomain_Id,
    pc.ProvisionControl_AuthorityReferenceId AuthorityProvision_Id,
    c.Controls_Name,
    c.Controls_Detail,
    c.Controls_Tags,
    c.Controls_Order,
    c.Controls_RiskStatus,
    c.Controls_Reference,
    c.Controls_RootControlId,
    c.Controls_ParentControlId,
    c.Controls_IsCurrent
from {{ ref("vwControls") }} c
left join {{ ref("vwPolicyDomain") }} pd on pd.PolicyDomain_Id = c.Controls_PolicyDomainId
left join {{ ref("vwProvisionControl") }} pc on pc.ProvisionControl_ControlsId = c.Controls_Id
