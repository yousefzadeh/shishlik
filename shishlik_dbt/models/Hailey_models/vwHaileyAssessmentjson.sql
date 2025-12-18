with base as(
    select 
        a.TenantId tenant_id,
        a.Id id,
        a.Name record_name,
        a.Description,
        a.CreationTime CreationDate,
        case
            when [WorkFlowId] = 1 then 'Requirement' when [WorkFlowId] = 0 then 'Question'
            else 'Undefined' end workflow,
        case
            when [QuestionType] = 0 then 'Preferred Answer'
            when [QuestionType] = 1 then 'Weighted Score'
            when [QuestionType] = 2 then 'Risk Rated'
            else 'Undefined' end assessment_style,
        a.DueDate,
        case 
            when a.HasPeriod = 1 then 'Yes' else 'No' end recurring,
        tv.Name respondent,
        e.Name product
    from {{ source("hailey_models", "Assessment") }} a
    left join {{ source("hailey_models", "TenantVendor") }} tv
        on tv.Id = a.TenantVendorId and tv.IsDeleted = 0 and tv.IsArchived = 0
    left join {{ source("hailey_models", "Engagement") }} e
        on e.Id = a.EngagementId and e.IsDeleted = 0
    where a.IsDeleted = 0
        and a.IsArchived = 0
        and a.IsTemplate = 0
        and a.IsDeprecatedAssessmentVersion = 0
)
, assess_tag as (
    select 
        at.TenantId, 
        at.AssessmentId, 
        STRING_AGG(t.Name, ', ') tag
    from {{ source("hailey_models", "AssessmentTag") }} at
    join {{ source("hailey_models", "Tags") }} t 
        on at.TagId = t.Id and at.TenantId = t.TenantId and t.IsDeleted = 0
    where at.IsDeleted = 0
    group by at.TenantId, at.AssessmentId
)
, assess_owner_base as (
    select 
        ao.TenantId,
        ao.AssessmentId,
        au.Name+' '+au.Surname owner
    from {{ source("hailey_models", "AssessmentOwner") }} ao
    left join {{ source("hailey_models", "AbpUsers") }} au 
        on ao.UserId = au.Id and au.IsDeleted = 0
    where ao.IsDeleted = 0

    union all

    select 
        ao.TenantId,
        ao.AssessmentId,
        aou. DisplayName owner
    from {{ source("hailey_models", "AssessmentOwner") }} ao
    left join {{ source("hailey_models", "AbpOrganizationUnits") }} aou 
        on ao.OrganizationUnitId = aou.Id and aou.IsDeleted = 0
    where ao.IsDeleted = 0
)
, assess_owner as (
    select 
        ao.TenantId, 
        ao.AssessmentId, 
        STRING_AGG(ao.owner, ', ') owner
    from assess_owner_base ao
    group by ao.TenantId, ao.AssessmentId
)
, assess_accmem_base as (
    select 
    aa.TenantId, 
    aa.AssessmentId, 
    au.Name+' '+au.Surname access_members
    from {{ source("hailey_models", "AssessmentAccessMember") }} aa
    left join {{ source("hailey_models", "AbpUsers") }} au 
        on aa.UserId = au.Id and au.IsDeleted = 0
    where aa.IsDeleted = 0

    union all

    select 
    aa.TenantId, 
    aa.AssessmentId, 
    aou. DisplayName access_members
    from {{ source("hailey_models", "AssessmentAccessMember") }} aa
    left join {{ source("hailey_models", "AbpOrganizationUnits") }} aou 
        on aa.OrganizationUnitId = aou.Id and aou.IsDeleted = 0
    where aa.IsDeleted = 0
)
, assess_accmem as (
    select aa.TenantId, aa.AssessmentId, STRING_AGG(aa.access_members, ', ') access_members
    from assess_accmem_base aa
    group by aa.TenantId, aa.AssessmentId
)

select
    a.tenant_id,
    a.id,
    a.record_name as Name,
    a.Description,
    a.CreationDate,
    a.workflow as Workflow,
    a.assessment_style as AssessmentStyle,
    a.Duedate,
    a.recurring as Recurring,
    a.respondent as Respondent,
    a.product as Product,
    at.tag as Tag,
    ao.owner as Owner,
    aa.access_members as AccessMembers
from base a
left join assess_tag at on at.AssessmentId = a.id
left join assess_owner ao on ao.AssessmentId = a.id
left join assess_accmem aa on aa.AssessmentId = a.id