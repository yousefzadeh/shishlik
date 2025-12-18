with access_member as (
select
acm.Uuid,
acm.TenantId,
acm.AssessmentId Assessment_Id,
acm.UserId AccessMemberId,
au.Name+' '+au.Surname Assessment_AccessMemberName
from {{ source("assessment_ref_models", "AssessmentAccessMember") }} acm
join {{ source("abp_ref_models", "AbpUsers") }} au
on au.Id = acm.UserId and au.IsDeleted = 0
where acm.IsDeleted = 0

union all

select
acm.Uuid,
acm.TenantId,
acm.AssessmentId Assessment_Id,
acm.OrganizationUnitId AccessMemberId,
aou.DisplayName Assessment_AccessMemberName
from {{ source("assessment_ref_models", "AssessmentAccessMember") }} acm
join {{ source("abp_ref_models", "AbpOrganizationUnits") }} aou
on aou.Id = acm.OrganizationUnitId and aou.IsDeleted = 0
where acm.IsDeleted = 0
)

select
Uuid,
TenantId,
Assessment_Id,
AccessMemberId Assessment_AccessMemberId,
Assessment_AccessMemberName
from access_member