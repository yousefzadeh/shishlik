{{ config(materialized="view") }}
with
    base as (
        select DISTINCT
            AssessmentTag_AssessmentId, 
            AssessmentTag_TagId, 
            AssessmentTag_TenantId,
            AssessmentTag_UpdateTime
        from {{ ref("vwAssessmentTag") }}
    ),
    TagsList as (
        select AssessmentTag_AssessmentId, 
        STRING_AGG(t.Tags_Name, ', ') Assessment_TagList,
        MAX(AssessmentTag_UpdateTime) as AssessmentTagList_UpdateTime
        from base
        join {{ ref("vwTags") }} t on t.Tags_ID = AssessmentTag_TagId
        group by AssessmentTag_AssessmentId
    )
select Tags.AssessmentTag_AssessmentId as Assessment_Id, 
Tags.Assessment_TagList,
AssessmentTagList_UpdateTime
from TagsList Tags
