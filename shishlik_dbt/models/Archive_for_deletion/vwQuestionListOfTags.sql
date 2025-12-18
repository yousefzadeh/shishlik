-- List of Tags per QuestionId
with
    qtj as (
        select distinct qtj.QuestionTags_QuestionId, qtj.Tags_Name
        from {{ ref("vwQuestionTagsJoined") }} as qtj
        where qtj.Tags_Name <> ''
    )
select qtj.QuestionTags_QuestionId, string_agg(qtj.Tags_Name, ', ') ListOfTags
from qtj
group by qtj.QuestionTags_QuestionId
