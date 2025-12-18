with
    users as (  -- Omit deleted users
        select AbpUsers_Id, AbpUsers_FullName from {{ ref("vwAbpUser") }}
    ),
    owner_user as (
        -- Assignee, Owner
        select distinct
            CONVERT(date, sr.CreationTime) as Task_StartDate,
            CONVERT(date, sr.StatementDueDate) as Task_DueDate,
            s.Statement_Title Action_Name,
            au.AbpUsers_FullName Assignee,
            aud.AbpUsers_FullName Owner,
            sr.Status StatementResponse_Status,
            case
                when sr.Status = 0
                then 'New'
                when sr.Status = 1
                then 'Completed'
                else 'In-Progress'
            end as StatusCode,
            sr.Id Task_Id,
            sr.StatementId StatementResponse_StatementId,
            sr.Response StatementResponse_Response,
            sr.UserId StatementResponse_UserId,
            sr.TenantId StatementResponse_TenantId,
            sr.CompletedDate StatementResponse_CompletedDate,
            sr.IsDeprecated StatementResponse_IsDeprecated,
            sr.DeletedForAssigneeRemoved StatementResponse_DeletedForAssigneeRemoved
        from {{ ref("vwStatement") }} s
        join {{ source("statement_models", "StatementResponse") }} sr on s.Statement_Id = sr.StatementId
        join {{ ref("vwStatementOwner") }} so on so.StatementOwner_StatementId = s.Statement_Id
        join users au on au.AbpUsers_Id = sr.UserId
        join {{ ref("vwAbpUser") }} aud on aud.AbpUsers_Id = so.StatementOwner_UserId
    ),
    owner_org as (
        -- Assignee, Organisation
        select distinct
            CONVERT(date, sr.CreationTime) as Task_StartDate,
            CONVERT(date, sr.StatementDueDate) as Task_DueDate,
            s.Statement_Title Action_Name,
            au.AbpUsers_FullName Assignee,
            aou.AbpOrganizationUnits_DisplayName Owner,
            sr.Status StatementResponse_Status,
            case
                when sr.Status = 0
                then 'New'
                when sr.Status = 1
                then 'Completed'
                else 'In-Progress'
            end as StatusCode,
            sr.Id Task_Id,
            sr.StatementId StatementResponse_StatementId,
            sr.Response StatementResponse_Response,
            sr.UserId StatementResponse_UserId,
            sr.TenantId StatementResponse_TenantId,
            sr.CompletedDate StatementResponse_CompletedDate,
            sr.IsDeprecated StatementResponse_IsDeprecated,
            sr.DeletedForAssigneeRemoved StatementResponse_DeletedForAssigneeRemoved
        from {{ ref("vwStatement") }} s
        join {{ source("statement_models", "StatementResponse") }} sr on s.Statement_Id = sr.StatementId
        join {{ ref("vwStatementOwner") }} so on so.StatementOwner_StatementId = s.Statement_Id
        join users au on au.AbpUsers_Id = sr.UserId
        join
            {{ ref("vwAbpOrganizationUnits") }} aou
            on aou.AbpOrganizationUnits_Id = so.StatementOwner_OrganizationUnitId
    ),
    owner_assigned as (
        select *
        from owner_user
        union all
        select *
        from owner_org
    ),
    owner_unassigned as (
        -- Assignee, No Owner
        select distinct
            CONVERT(date, sr.CreationTime) as Task_StartDate,
            CONVERT(date, sr.StatementDueDate) as Task_DueDate,
            s.Statement_Title Action_Name,
            au.AbpUsers_FullName Assignee,
            'Unassigned' Owner,
            sr.Status StatementResponse_Status,
            case
                when sr.Status = 0
                then 'New'
                when sr.Status = 1
                then 'Completed'
                else 'In-Progress'
            end as StatusCode,
            sr.Id Task_Id,
            sr.StatementId StatementResponse_StatementId,
            sr.Response StatementResponse_Response,
            sr.UserId StatementResponse_UserId,
            sr.TenantId StatementResponse_TenantId,
            sr.CompletedDate StatementResponse_CompletedDate,
            sr.IsDeprecated StatementResponse_IsDeprecated,
            sr.DeletedForAssigneeRemoved StatementResponse_DeletedForAssigneeRemoved
        from {{ ref("vwStatement") }} s
        join {{ source("statement_models", "StatementResponse") }} sr on s.Statement_Id = sr.StatementId
        join users au on au.AbpUsers_Id = sr.UserId
        where not exists (select 1 from owner_assigned oa where oa.task_id in (sr.Id))
    ),
    Task_Details as (
        select *
        from owner_assigned
        union all
        select *
        from owner_unassigned
    )
select
    Task_StartDate,
    Task_DueDate,
    Action_Name,
    Assignee,
    Owner,
    StatementResponse_Status,
    StatusCode,
    Task_Id,
    StatementResponse_StatementId,
    StatementResponse_Response,
    StatementResponse_UserId,
    StatementResponse_TenantId,
    StatementResponse_CompletedDate,
    StatementResponse_IsDeprecated
from Task_Details td
where StatementResponse_IsDeprecated = 0 and StatementResponse_DeletedForAssigneeRemoved = 0
