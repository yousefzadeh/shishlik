{{ config(materialized="view") }}
-- Convenience view alias to QuestionVendorUser
select *
from {{ ref("vwQuestionVendorUser") }}
