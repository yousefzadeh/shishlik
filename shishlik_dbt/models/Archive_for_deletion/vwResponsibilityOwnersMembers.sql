with
    owner as (select * from {{ ref("vwStatementOwner") }}),
    members as (select * from {{ ref("vwStatementMember") }}),
    owner_list as (
        select
            StatementOwner_TenantId Tenant_Id,
            StatementOwner_StatementId Responsibility_Id,
            string_agg(u.AbpUsers_FullName, ', ') OwnerList
        from owner
        join {{ ref("vwAbpUser") }} u on owner.StatementOwner_UserId = u.AbpUsers_Id
        group by StatementOwner_TenantId, StatementOwner_StatementId
    ),
    members_list as (
        select
            StatementMember_TenantId Tenant_Id,
            StatementMember_StatementId Responsibility_Id,
            string_agg(u.AbpUsers_FullName, ', ') MembersList
        from members
        join {{ ref("vwAbpUser") }} u on members.StatementMember_UserId = u.AbpUsers_Id
        group by StatementMember_TenantId, StatementMember_StatementId
    )
select o.Tenant_Id, o.Responsibility_Id, o.OwnerList, m.MembersList
from owner_list o
left join
    members_list m
    on o.Tenant_Id = m.Tenant_Id
    and o.Responsibility_Id = m.Responsibility_Id
