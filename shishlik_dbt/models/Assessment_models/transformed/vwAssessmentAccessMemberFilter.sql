with
    access_user as (
        select acm.AssessmentAccessMember_AssessmentId
        , u.AbpUsers_FullName AccessMemberName
        , acm.AssessmentAccessMember_UpdateTime
        from {{ ref("vwAssessmentAccessMember") }} acm
        join {{ ref("vwAbpUser") }} u on acm.AssessmentAccessMember_UserId = u.AbpUsers_Id
    ),
    access_org as (
        select acm.AssessmentAccessMember_AssessmentId
        , o.AbpOrganizationUnits_DisplayName AccessMemberName
        , acm.AssessmentAccessMember_UpdateTime
        from {{ ref("vwAssessmentAccessMember") }} acm
        join {{ ref("vwAbpOrganizationUnits") }} o on acm.AssessmentAccessMember_OrganizationUnitId = o.AbpOrganizationUnits_Id
    ),
    final as (
        select *
        from access_user
        union all
        select *
        from access_org
    )
select *
from final
