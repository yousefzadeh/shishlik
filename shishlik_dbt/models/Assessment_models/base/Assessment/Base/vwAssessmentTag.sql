{{ config(materialized='view') }}
WITH base AS (
SELECT
    {{ system_fields_macro() }}
    ,[TagId]
    ,[AssessmentId]
    ,[TenantId]
    , coalesce([LastModificationTime],[CreationTime]) as base_UpdateTime
FROM {{ source('assessment_models', 'AssessmentTag') }}
{{ system_remove_IsDeleted() }}
)

, Tags as (
select base.AssessmentId, 
STRING_AGG(t.Tags_Name,', ') Assessment_TagName,
max(Tags_UpdateTime)as Tags_UpdateTime
from base
left join {{ref('vwTags')}} t
on t.Tags_ID = base.TagId

group by 
base.AssessmentId
)

SELECT
    {{ col_rename("ID", "AssessmentTag") }},
    {{ col_rename("CreationTime", "AssessmentTag") }},
    {{ col_rename("CreatorUserId", "AssessmentTag") }},
    {{ col_rename("TagId", "AssessmentTag") }},
    base.{{ col_rename("AssessmentId", "AssessmentTag") }},
    {{ col_rename("TenantId", "AssessmentTag") }},
    Tags.Assessment_TagName,
    cast(IIF (base_UpdateTime>Tags_UpdateTime, base_UpdateTime, Tags_UpdateTime)  as datetime2) as AssessmentTag_UpdateTime
FROM base
left join Tags
on Tags.AssessmentId = base.AssessmentId
