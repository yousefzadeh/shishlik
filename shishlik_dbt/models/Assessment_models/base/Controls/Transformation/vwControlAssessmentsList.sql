-- Control Set to QBA
with
    controlset_assessment as (
        select distinct
            pol.Policy_TenantId ControlSet_TenantId,
            pol.Policy_Id ControlSet_Id,
            ass.Assessment_Name,
            case
                when len(ass.Assessment_Name) > 33
                then
                    left(ass.Assessment_Name, 20)
                    + '...'
                    + right(ass.Assessment_Name, 10)
                else ass.Assessment_Name
            end Assessment_ShortName
        from {{ ref("vwPolicy") }} pol
        inner join
            {{ ref("vwAssessment") }} ass
            on pol.Policy_Id = ass.Assessment_PolicyId
            and pol.Policy_TenantId = ass.Assessment_TenantId
    ),
    final as (
        select
            ControlSet_TenantId,
            ControlSet_Id,
            string_agg(cast(Assessment_ShortName as varchar(max)), ', ') Assessment_List
        from controlset_assessment
        group by ControlSet_TenantId, ControlSet_Id
    )
select *
from final
