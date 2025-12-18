with
    users as (
        select
            rro.IssueOwner_TenantId RegisterRecordOwner_TenantId,
            rro.IssueOwner_IssueId RegisterRecordOwner_RegisterRecordId,
            rro.IssueOwner_UserId Owner_Id,
            rro.IssueOwner_FullName RegisterRecord_Owner
        from {{ ref("vwIssueOwner") }} rro
        where rro.IssueOwner_FullName is not null

        union all

        select
            rro.IssueOwner_TenantId RegisterRecordOwner_TenantId,
            rro.IssueOwner_IssueId RegisterRecordOwner_RegisterRecordId,
            rro.IssueOwner_OrganizationUnitId Owner_Id,
            rro.IssueOwner_OrganisationName RegisterRecord_Owner
        from {{ ref("vwIssueOwner") }} rro
        where rro.IssueOwner_OrganisationName is not null
    ),
    OwnerList as (
        select
            u.RegisterRecordOwner_RegisterRecordId,
            left(STRING_AGG(cast(u.RegisterRecord_Owner as nvarchar(max)), ', '), 4000) RegisterRecord_OwnerList
        from users u
        -- where u.RegisterRecordOwner_RegisterRecordId = 3020
        group by u.RegisterRecordOwner_RegisterRecordId
    )

select
    u.RegisterRecordOwner_TenantId,
    u.RegisterRecordOwner_RegisterRecordId,
    u.Owner_Id,
    u.RegisterRecord_Owner,
    ol.RegisterRecord_OwnerList
from users u
left join
    OwnerList ol on ol.RegisterRecordOwner_RegisterRecordId = u.RegisterRecordOwner_RegisterRecordId
    -- where u.RegisterRecordOwner_TenantId = 2034
    
