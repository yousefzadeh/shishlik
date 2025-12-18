with
    reviewer_user as (
        select ao.QuestionUser_TenantId, ao.QuestionUser_QuestionId, u.AbpUsers_FullName ReviewerName
        from {{ ref("vwQuestionUser") }} ao
        join {{ ref("vwAbpUser") }} u on ao.QuestionUser_Userid = u.AbpUsers_Id
    ),
    reviewer_org as (
        select ao.QuestionUser_TenantId, ao.QuestionUser_QuestionId, o.AbpOrganizationUnits_DisplayName ReviewerName
        from {{ ref("vwQuestionUser") }} ao
        join {{ ref("vwAbpOrganizationUnits") }} o on ao.QuestionUser_OrganizationUnitId = o.AbpOrganizationUnits_Id
    ),
    final as (
        select *
        from reviewer_user
        union all
        select *
        from reviewer_org
    ),
    list as (
        select QuestionUser_QuestionId, string_agg(ReviewerName, ', ') as ReviewerList
        from final
        group by QuestionUser_QuestionId
    )
select final.*, list.ReviewerList
from final
left join list on list.QuestionUser_QuestionId = final.QuestionUser_QuestionId