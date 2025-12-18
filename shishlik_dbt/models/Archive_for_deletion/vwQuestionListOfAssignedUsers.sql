-- List of Assigned Users per QuestionId
with
    qvu as (
        select distinct
            qvu.QuestionVendorUser_QuestionId, trim(au.AbpUsers_FullName) AssignedUserFullName, au.AbpUsers_UserName
        from {{ ref("vwQuestionVendorUser") }} as qvu
        inner join {{ ref("vwAbpUser") }} as au on qvu."QuestionVendorUser_ID" = au."AbpUsers_Id"
        where trim(au.AbpUsers_FullName) <> ''
    )
select qvu.QuestionVendorUser_QuestionId, string_agg(qvu.AssignedUserFullName, ', ') ListOfAssignedUsers
from qvu
group by qvu.QuestionVendorUser_QuestionId
