with
    base1 as (
        select
            cs.TenantId Tenant_Id,
            c.EntityId,
            DATEADD(mi, DATEDIFF(mi, 0, c.ChangeTime), 0) Date_Time,
            'User Management' EventType,
            'User Password Reset' Event

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        join {{ source("assessment_models", "AbpUsers") }} au on au.Id = c.EntityId
        -- where cs.TenantId = 1384
        where
            c.EntityTypeFullName = 'LegalRegTech.Authorization.Users.User'
            and pc.PropertyNameVarChar = 'PasswordResetCode'
            and pc.OriginalValue is not null
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

    )

select distinct
    base1.Tenant_Id,
    base1.Date_Time,
    base1.EventType,
    base1.Event,
    base2.Actioned_by,
    base2.Impacted + ' reset password' Changed,
    base2.Impacted

from base1
left join
    base2 on base1.Tenant_Id = base2.Tenant_Id and base1.EntityId = base2.EntityId

    -- where base1.Tenant_Id = 1384
    
