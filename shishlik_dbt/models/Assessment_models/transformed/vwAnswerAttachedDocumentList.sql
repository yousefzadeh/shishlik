with
    docs as (
        select distinct 
            ad.AnswerDocument_AnswerId
            , ad.AnswerDocument_TenantId
            , ad.AnswerDocument_DisplayFileName
            , ad.AnswerDocument_UpdateTime
        from {{ ref("vwAnswerDocument") }} ad 
    ),
    list as (
        select
            AnswerDocument_AnswerId Answer_Id,
            AnswerDocument_TenantId Answer_TenantId,
            string_agg(AnswerDocument_DisplayFileName, ', ') as AnswerDocument_DisplayFileName,
            max(AnswerDocument_UpdateTime) as AnswerDocument_UpdateTime
        from docs
        group by AnswerDocument_AnswerId, AnswerDocument_TenantId
    )
select *
from list
