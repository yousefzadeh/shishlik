with
    final as (
        select *
        from {{ ref("vwAssessmentAuthorityQuestionAnswer_authority") }}
        union all
        select *
        from {{ ref("vwAssessmentAuthorityQuestionAnswer_controlset") }}
        union all
        select *
        from {{ ref("vwAssessmentAuthorityQuestionAnswer_unlinked") }}
    )
select *
from final
