with base as(
select 
r.TenantId,
abp.Name Tenant_Name,
r.Id Risk_Id,
r.TenantEntityUniqueId Risk_IdRef,
r.Name Risk_Name,
rtp.Id RiskTreatmentPlan_Id,
rtp.TreatmentName RiskTreatmentPlan_Name,
rtp.IsDeleted,
rtp.TreatmentDescription,
rtp.TreatmentDate DueDate,
--rtp.TreatmentProgress,
--rtp.IsDeprecated,
case
when rtp.Status = 0 then 'New' when rtp.Status = 1 then 'Completed' when rtp.Status = 3 then 'In-Progress'
end as RiskTreatmentPlan_Status,
case
when rtp.Status in (1)
then 'No'
when rtp.Status in (0, 3) and getdate() <= TreatmentDate
then 'No'
when getdate() > TreatmentDate
then 'Yes'
else 'No'
end as IsOverDue,
rtp.TreatmentCompletedDate,
rtpc.Comment

from {{ source("risk_models", "Risk") }} r
join {{ source("assessment_models", "AbpTenants") }} abp on abp.Id = r.TenantId
join {{ source("risk_models", "RiskTreatmentPlanAssociation") }} rtpa
on rtpa.RiskId = r.Id and rtpa.IsDeleted = 0
join {{ source("risk_models", "RiskTreatmentPlan") }} rtp
on rtp.Id = rtpa.RiskTreatmentPlanId and rtp.IsDeleted = 0
left join {{ source("risk_models", "RiskTreatmentPlanComment") }} rtpc
on rtpc.RiskTreatmentPlanId = rtp.Id and rtpc.IsDeleted = 0

where r.IsDeleted = 0
and abp.IsDeleted = 0 and abp.IsActive = 1
and r.[Status] != 100
)
, assignee as (
select
rtpo.TenantId,
rtpo.RiskTreatmentPlanId,
STRING_AGG(coalesce(u.[Name], aou.DisplayName), ', ') as AssigneeList

from base r
left join {{ source("risk_models", "RiskTreatmentPlanOwner") }} rtpo
on rtpo.RiskTreatmentPlanId = r.RiskTreatmentPlan_Id
left join {{ source("assessment_models", "AbpUsers") }} u on rtpo.UserId = u.Id
left join {{ source("assessment_models", "AbpOrganizationUnits") }} aou
on rtpo.OrganizationUnitId = aou.Id
and rtpo.TenantId = aou.TenantId
group by rtpo.TenantId, rtpo.RiskTreatmentPlanId
)

select r.*, a.AssigneeList
from base r
left join assignee a on a.TenantId = r.TenantId and a.RiskTreatmentPlanId = r.RiskTreatmentPlan_Id