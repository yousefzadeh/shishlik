{{ config(materialized="view") }}

select *
from {{ ref("vwAssessment") }} ass
where ass.Assessment_Workflow = 'Requirement' and ass.Assessment_IsTemplate = 0
