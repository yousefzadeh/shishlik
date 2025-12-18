select * 
, NULL NullScore
, NULL NullComments
, GREATEST(Answer_UpdateTime, Question_UpdateTime, QuestionGroup_UpdateTime, QuestionGroupResponse_UpdateTime)  as QGA_Updatetime
from {{ ref("vwQuestionAll") }} q
join {{ ref("vwQuestionGroup") }} qg on qg.QuestionGroup_ID = q.Question_QuestionGroupId
join {{ ref("vwQuestionGroupResponse") }} qgr on qgr.QuestionGroupResponse_ID = q.Question_QuestionGroupResponseId
join {{ ref("vwAnswer") }} a on a.Answer_QuestionId = q.Question_Id
    -- where q.Question_TenantId = 1384
    
