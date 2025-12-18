{{ config(materialized="view") }}

select *
from {{ ref("vwAssessment") }} ass
where ass.Assessment_Workflow = 'Question' and ass.Assessment_IsTemplate = 0
