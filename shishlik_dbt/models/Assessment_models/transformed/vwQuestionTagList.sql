{{ config(materialized="view") }}
with
    question_base as (
        select QuestionTags_QuestionId, QuestionTags_TagId, QuestionTags_TenantId, QuestionTags_UpdateTime from {{ ref("vwQuestionTags") }}
    ),
     tags_base as (
        select Tags_ID, Tags_Name, Tags_UpdateTime from {{ ref("vwTags") }}
    ),
    LastModifiedDates as (
	select 
        a.Date_id
        , max(a.LastModificationTime) as max_LastModificationTime
	from
		(
		select QuestionTags_TagId as Date_id, QuestionTags_UpdateTime as LastModificationTime from question_base
		union all
		select Tags_ID as Date_id, Tags_UpdateTime as LastModificationTime from tags_base
		)a
     group by Date_id
	),
    TagsList as (
        select q.QuestionTags_QuestionId
            , STRING_AGG(t.Tags_Name, ', ') Question_TagList
            , max(max_LastModificationTime) QuestionTagsList_UpdateTime
        from question_base q
        join tags_base t on t.Tags_ID = q.QuestionTags_TagId
        join LastModifiedDates lmd on lmd.date_id = q.QuestionTags_TagId
        group by q.QuestionTags_QuestionId
    )
select 
    Tags.QuestionTags_QuestionId as Question_Id
    , Tags.Question_TagList
    , QuestionTagsList_UpdateTime
from TagsList Tags
