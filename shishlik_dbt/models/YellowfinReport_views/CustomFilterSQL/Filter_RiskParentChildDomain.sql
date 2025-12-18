with
    risk_domain as (
        select Risk_Id, CustomField_Value Domain, Tenant_Id
        from {{ ref("vwRiskCustomFieldValue") }} domain
        where CustomField_InternalDefaultName = 'RiskDomain'
    ),
    child_domain as (
        select Risk_Id, CustomField_Level2Value ChildDomain, Tenant_Id
        from {{ ref("vwRiskChildCustomFieldValue") }} child
        where CustomField_InternalDefaultName = 'RiskDomain'
    ),
    grandchild_domain as (
        select Risk_Id, CustomField_Level3Value GrandChildDomain, Tenant_Id
        from {{ ref("vwRiskGrandChildCustomFieldValue") }} grandchild
        where CustomField_InternalDefaultName = 'RiskDomain'
    ),
    final as (
        select distinct
        r.Risk_TenantId Tenant_Id,
        r.Risk_Name,
        rd.Domain,
        cd.ChildDomain,
        gcd.GrandChildDomain
        from risk_domain rd 
        join child_domain cd on rd.Tenant_Id = cd.Tenant_Id and rd.Risk_Id = cd.Risk_Id
        join grandchild_domain gcd on cd.Tenant_Id = gcd.Tenant_Id and cd.Risk_Id = gcd.Risk_Id
        join {{ ref("vwRisk") }} r on r.Risk_Id = rd.Risk_Id and r.Risk_TenantId = rd.Tenant_Id
    )
select
Tenant_Id,
Risk_Name,
Domain,
ChildDomain,
GrandChildDomain
from final
{# where r.Risk_TenantId = 1838 #}
 