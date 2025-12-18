{{ config(materialized="view") }}
with
    questionTags as (
        select
            [QuestionTags_ID],
            [QuestionTags_CreationTime],
            [QuestionTags_CreatorUserId],
            [QuestionTags_TenantId],
            [QuestionTags_QuestionId],
            [QuestionTags_TagId],
                [QuestionTags_UpdateTime]
        from {{ ref("vwQuestionTags") }}
    ),
       tags as (
        select 
            [Tags_ID],
            [Tags_Name],
            [Tags_Description],
            [Tags_Type],
            [Tags_TenantId],
            [Tags_UpdateTime]
        from {{ ref("vwTags") }}
    ), 
    LastModifiedDates as (
	select a.Date_id
	, max(a.LastModificationTime) as max_LastModificationTime
	from
		(
		select QuestionTags_TagId as date_id, QuestionTags_UpdateTime as LastModificationTime from questionTags
		union all
		select Tags_ID as date_id, Tags_UpdateTime as LastModificationTime from Tags
		)a
    group by date_id
	),
    TagList as (
        select
             qt. [QuestionTags_QuestionId],
            left(STRING_AGG(cast(t. [Tags_Name] as nvarchar(max)), ', '), 4000) TagName_List,
            max(max_LastModificationTime) as [QuestionTagsJoined_UpdateTime]
        -- tags.[Tags_Name]
        from questionTags qt
        join LastModifiedDates lmd on lmd.date_id = qt.QuestionTags_TagId
        left join tags t on qt. [QuestionTags_TagId] = t. [Tags_ID]
        -- where t.[Tags_TenantId] = 1384 and qt.[QuestionTags_QuestionId] = 1303193
        group by qt. [QuestionTags_QuestionId]
    ),
    joined_tags as (
        select
            QuestionTags. [QuestionTags_ID],
            questionTags. [QuestionTags_TagId],
            questionTags. [QuestionTags_QuestionId],
            questionTags. [QuestionTags_CreationTime],
            questionTags. [QuestionTags_CreatorUserId],
            tags. [Tags_Name],
            TagList. [TagName_List],
            tags. [Tags_Description],
            tags. [Tags_Type],
             tags. [Tags_TenantId],
            TagList. [QuestionTagsJoined_UpdateTime]  
        from questionTags
        left join tags on questionTags. [QuestionTags_TagId] = tags. [Tags_ID]
        left join TagList on TagList.QuestionTags_QuestionId = questionTags. [QuestionTags_QuestionId]
    )

select 
[QuestionTags_ID]
,[QuestionTags_TagId]
,[QuestionTags_QuestionId]
,[QuestionTags_CreationTime]
,[QuestionTags_CreatorUserId]
,[Tags_Name]
,[TagName_List]
,[Tags_Description]
,[Tags_Type]
,[Tags_TenantId]
,[QuestionTagsJoined_UpdateTime]
from joined_tags
