with
    rrr as (
        select rrr.RiskRegisterRecord_RiskId, r.Register_RegisterName, rr.RegisterRecord_Name
        from {{ ref("vwRiskRegisterRecord") }} rrr
        join {{ ref("vwRegisterRecord") }} rr on rrr.RiskRegisterRecord_RegisterRecordId = rr.RegisterRecord_Id
        join {{ ref("vwRegister") }} r on rr.RegisterRecord_RegisterId = r.Register_Id
        group by rrr.RiskRegisterRecord_RiskId, r.Register_RegisterName, rr.RegisterRecord_Name
    ),
    recordlist as (
        select
            rrr.RiskRegisterRecord_RiskId Register_RiskId,
            rrr.Register_RegisterName Register_Name,
            left(
                string_agg(cast('"' + rrr.RegisterRecord_Name + '"' as nvarchar(max)), ', '), 4000
            ) as Register_RecordList
        from rrr
        group by rrr.RiskRegisterRecord_RiskId, rrr.Register_RegisterName
    ),
    register_recordlist as (
        select
            T.Register_RiskId,
            string_agg('[' + T.Register_Name + ' = ' + Register_RecordList + ']', ', ') as RegisterList
        from recordlist as T
        group by T.Register_RiskId
    )
select *
from register_recordlist
