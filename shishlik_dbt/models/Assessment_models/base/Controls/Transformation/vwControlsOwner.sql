with ControlOwnersBase as(
select distinct co.ControlOwner_Id, co.ControlOwner_TenantId, co.ControlOwner_ControlId, 
cast(au.AbpUsers_FullName as varchar) as ControlOwnerUsers, 
cast(aou.AbpOrganizationUnits_DisplayName as varchar) ControlOwnerOrgs
from {{ ref("vwControlOwner") }} co
left join {{ ref("vwAbpUser") }} au on au.AbpUsers_Id = co.ControlOwner_UserId
left join {{ ref("vwAbpOrganizationUnits") }} aou on aou.AbpOrganizationUnits_Id = co.ControlOwner_OrganizationUnitId
join {{ ref("vwControls") }} c on c.Controls_Id = co.ControlOwner_ControlId
join {{ ref("vwPolicyDomain") }} pd on pd.PolicyDomain_Id = c.Controls_PolicyDomainId
join {{ ref("vwPolicy") }} p on p.Policy_Id = pd.PolicyDomain_PolicyId
where p.Policy_Status != 100 and p.Policy_IsCurrent = 1
)

select owners.*
from
ControlOwnersBase cob
unpivot (ControlOwners for Owners in (ControlOwnerUsers, ControlOwnerOrgs)) as owners