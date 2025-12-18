-- List of Responsibilities with Owner type and Owner Name
select
    ro.ResponsibilityOwner_Id ResponsibilityOwner_Id,
    ro.ResponsibilityOwner_ResponsibilityId Responsibility_Id,
    'Owner User' ResponsibilityOwner_OwnerType,
    ResponsibilityOwner_UserId ResponsibilityOwner_OwnerId,
    COALESCE(
        ou.AbpUsers_FullName, '#' + cast(ResponsibilityOwner_UserId as varchar(10)) + ' User name not found'
    ) ResponsibilityOwner_Name
from {{ ref("vwResponsibilityOwner") }} ro
join {{ ref("vwAbpUser") }} ou on ro.ResponsibilityOwner_UserId = ou.AbpUsers_Id

union all

select
    ro.ResponsibilityOwner_Id,
    ro.ResponsibilityOwner_ResponsibilityId,
    'Owner Organisation' ResponsibilityOwner_OwnerType,
    ResponsibilityOwner_OrganizationUnitId ResponsibilityOwner_OwnerId,
    COALESCE(
        oo.AbpOrganizationUnits_DisplayName,
        '#' + cast(ResponsibilityOwner_UserId as varchar(10)) + ' Organization name not found'
    ) ResponsibilityOwner_Name
from {{ ref("vwResponsibilityOwner") }} ro
join {{ ref("vwAbpOrganizationUnits") }} oo on ro.ResponsibilityOwner_OrganizationUnitId = oo.AbpOrganizationUnits_Id
where ro.ResponsibilityOwner_UserId is null

union all

select
    ro.ResponsibilityOwner_Id,
    ro.ResponsibilityOwner_ResponsibilityId,
    'Owner Unassigned' Responsibility_OwnerType,
    0 Responsibility_OwnerId,
    'Owner Unassigned' Responsibility_OwnerName
from {{ ref("vwResponsibilityOwner") }} ro
where ro.ResponsibilityOwner_UserId is NULL and ro.ResponsibilityOwner_OrganizationUnitId is NULL
