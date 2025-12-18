{{- config(materialized="view") -}}
with
    final as (
        select *
        from {{ ref("vwAssessmentAuthority_authority") }}
        union all
        select *
        from {{ ref("vwAssessmentAuthority_controlset") }}
        union all
        select *
        from {{ ref("vwAssessmentAuthority_unlinked") }}
    )
select *
from final
