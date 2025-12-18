with
    users as (
        select
            rro.RegisterAccessMember_TenantId,
            rro.RegisterAccessMember_RegisterId,
            rro.RegisterAccessMember_User RegisterAccessMember
        from {{ ref("vwRegisterAccessMember") }} rro
        where rro.RegisterAccessMember_User is not null

        union all

        select
            rro.RegisterAccessMember_TenantId,
            rro.RegisterAccessMember_RegisterId,
            rro.RegisterAccessMember_Organization RegisterAccessMember
        from {{ ref("vwRegisterAccessMember") }} rro
        where rro.RegisterAccessMember_Organization is not null
    ),
    OwnerList as (
        select
            u.RegisterAccessMember_RegisterId,
            left(STRING_AGG(cast(u.RegisterAccessMember as nvarchar(max)), ', '), 4000) RegisterAccessMemberList
        from users u
        -- where u.RegisterRecordOwner_RegisterRecordId = 3020
        group by u.RegisterAccessMember_RegisterId
    )

select
    u.RegisterAccessMember_TenantId,
    u.RegisterAccessMember_RegisterId,
    u.RegisterAccessMember,
    ol.RegisterAccessMemberList
from users u
left join
    OwnerList ol on ol.RegisterAccessMember_RegisterId = u.RegisterAccessMember_RegisterId
    -- where u.RegisterAccessMember_RegisterId = 1802
    
