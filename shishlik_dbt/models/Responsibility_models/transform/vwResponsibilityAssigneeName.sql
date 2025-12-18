-- List of Responsibilities with Assignee type and Assignee Name
select
    ro.ResponsibilityAssignee_Id ResponsibilityAssignee_Id,
    ro.ResponsibilityAssignee_ResponsibilityId Responsibility_Id,
    'Assignee User' ResponsibilityAssignee_AssigneeType,
    ResponsibilityAssignee_UserId ResponsibilityAssignee_AssigneeId,
    COALESCE(
        ou.AbpUsers_FullName, '#' + cast(ResponsibilityAssignee_UserId as varchar(10)) + ' User name not found'
    ) ResponsibilityAssignee_Name
from {{ ref("vwResponsibilityAssignee") }} ro
join {{ ref("vwAbpUser") }} ou on ro.ResponsibilityAssignee_UserId = ou.AbpUsers_Id

union all

select
    ro.ResponsibilityAssignee_Id,
    ro.ResponsibilityAssignee_ResponsibilityId,
    'Assignee Organisation' ResponsibilityAssignee_AssigneeType,
    ResponsibilityAssignee_OrganizationUnitId ResponsibilityAssignee_AssigneeId,
    COALESCE(
        oo.AbpOrganizationUnits_DisplayName,
        '#' + cast(ResponsibilityAssignee_UserId as varchar(10)) + ' Organization name not found'
    ) ResponsibilityAssignee_Name
from {{ ref("vwResponsibilityAssignee") }} ro
join {{ ref("vwAbpOrganizationUnits") }} oo on ro.ResponsibilityAssignee_OrganizationUnitId = oo.AbpOrganizationUnits_Id
where ro.ResponsibilityAssignee_UserId is null

union all

select
    ro.ResponsibilityAssignee_Id,
    ro.ResponsibilityAssignee_ResponsibilityId,
    'Assignee Unassigned' Responsibility_AssigneeType,
    0 Responsibility_AssigneeId,
    'Assignee Unassigned' Responsibility_AssigneeName
from {{ ref("vwResponsibilityAssignee") }} ro
where ro.ResponsibilityAssignee_UserId is NULL and ro.ResponsibilityAssignee_OrganizationUnitId is NULL
