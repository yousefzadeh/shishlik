with owner as (
select
ao.Uuid,
ao.TenantId,
ao.AssessmentId Assessment_Id,
ao.UserId OwnerId,
au.Name+' '+au.Surname Assessment_OwnerName
from {{ source("assessment_ref_models", "AssessmentOwner") }} ao
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = ao.UserId and au.IsDeleted = 0
where ao.IsDeleted = 0

union all

select
ao.Uuid,
ao.TenantId,
ao.AssessmentId Assessment_Id,
ao.OrganizationUnitId OwnerId,
aou.DisplayName Assessment_OwnerName
from {{ source("assessment_ref_models", "AssessmentOwner") }} ao
join {{ source("abp_ref_models", "AbpOrganizationUnits") }} aou
on aou.Id = ao.OrganizationUnitId and aou.IsDeleted = 0
where ao.IsDeleted = 0
)

select
Uuid,
TenantId,
Assessment_Id,
OwnerId Assessment_OwnerId,
Assessment_OwnerName
from owner