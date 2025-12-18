with
    base1 as (
        select
            cs.TenantId Tenant_Id,
            c.EntityId,
            c.ChangeTime Date_Time,
            'User Management' EventType,
            'User Role Added' Event,
            ar.DisplayName roles

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpRoles") }} ar on ar.Id = pc.NewValue
        -- where cs.TenantId = 1384
        where c.EntityTypeFullName = 'Abp.Authorization.Users.UserRole' and pc.PropertyNameVarChar = 'RoleId'  -- and pc.NewValue = 'false'
    ),
    base2 as (
        select cs.TenantId Tenant_Id, c.EntityId, au.Name + ' ' + au.Surname Actioned_by

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = pc.NewValue
        -- where cs.TenantId = 1384
        where c.EntityTypeFullName = 'Abp.Authorization.Users.UserRole' and pc.PropertyNameVarChar = 'CreatorUserId'
    ),
    base3 as (
        select cs.TenantId Tenant_Id, c.EntityId, au.UserName Impacted

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = pc.NewValue
        -- where cs.TenantId = 1384
        where c.EntityTypeFullName = 'Abp.Authorization.Users.UserRole' and pc.PropertyNameVarChar = 'UserId'
    )

select distinct
    base1.Tenant_Id,
    base1.Date_Time,
    base1.EventType,
    base1.Event,
    base2.Actioned_by,
    base3.Impacted + + ' assigned role ' + base1.roles Changed,
    base3.Impacted Impacted
from base1
left join base2 on base1.Tenant_Id = base2.Tenant_Id and base1.EntityId = base2.EntityId
left join
    base3 on base1.Tenant_Id = base3.Tenant_Id and base1.EntityId = base3.EntityId

    -- where base1.Tenant_Id = 1384
    
