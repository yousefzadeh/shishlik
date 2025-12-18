with
    base1 as (
        select
            cs.TenantId Tenant_Id,
            c.Id as [ChangeId],
            c.EntityId,
            c.ChangeTime Date_Time,
            'User Management' EventType,
            pc.NewValue Lockout

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        join {{ source("assessment_models", "AbpUsers") }} au on au.Id = c.EntityId
        -- where cs.TenantId = 1384
        where
            c.EntityTypeFullName = 'LegalRegTech.Authorization.Users.User'
            and pc.PropertyNameVarChar = 'LockoutEndDateUtc'
    ),
    base2 as (
        select
            cs.TenantId Tenant_Id,
            c.EntityId,
            au.Name + ' ' + au.Surname Actioned_by,
            au.Name + ' ' + au.Surname Impacted

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = c.EntityId
        -- where cs.TenantId = 1384
        where c.EntityTypeFullName = 'LegalRegTech.Authorization.Users.User'
    ),
    base3 as (
        select
            cs.TenantId Tenant_Id, c.Id as [ChangeId], c.EntityId, c.ChangeTime, au.Name + ' ' + au.Surname Actioned_by

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = cs.UserId
        -- where cs.TenantId = 1384
        where
            c.EntityTypeFullName = 'LegalRegTech.Authorization.Users.User'
            and pc.PropertyNameVarChar = 'LockoutEndDateUtc'
            and pc.NewValue is null
    )

select distinct
    case
        when base1.Lockout is not null then base1.Tenant_Id when base1.Lockout is null then base1.Tenant_Id
    end Tenant_Id,
    case
        when base1.Lockout is not null then base1.Date_Time when base1.Lockout is null then base1.Date_Time
    end Date_Time,
    base1.EventType,
    case when base1.Lockout is not null then 'User Locked' when base1.Lockout is null then 'User Unlocked' end Event,
    case
        when base1.Lockout is not null then base2.Actioned_by when base1.Lockout is null then base3.Actioned_by
    end Actioned_by,
    case
        when base1.Lockout is not null
        then base2.Impacted + ' locked'
        when base1.Lockout is null
        then base2.Impacted + ' unlocked'
    end Changed,
    case when base1.Lockout is not null then base2.Impacted when base1.Lockout is null then base2.Impacted end Impacted

from base1
left join base2 on base1.Tenant_Id = base2.Tenant_Id and base1.EntityId = base2.EntityId
left join
    base3 on base2.Tenant_Id = base3.Tenant_Id and base2.EntityId = base3.EntityId and base1.ChangeId = base3.ChangeId

    -- where base1.Tenant_Id = 1384
    
