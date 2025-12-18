with
    base1 as (
        select distinct
            cs.TenantId Tenant_Id,
            c.EntityId,
            cs.Id ChangeId,
            c.ChangeTime Date_Time,
            'Role Management' EventType,
            'Role Deleted' Event,
            -- case when pc.PropertyNameVarChar = 'IsGranted' then pc.NewValue end GrantPermission
            ar.DisplayName Impacted

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpRoles") }} ar on ar.Id = c.EntityId
        -- where cs.TenantId = 1384
        where
            c.EntityTypeFullName = 'LegalRegTech.Authorization.Roles.Role'
            and pc.PropertyNameVarChar = 'IsDeleted'
            and pc.NewValue = 'true'
    ),
    base2 as (
        select distinct cs.TenantId Tenant_Id, c.EntityId, cs.Id ChangeId, au.Name + ' ' + au.Surname Actioned_by

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = cs.UserId
        -- where cs.TenantId = 1384
        where c.EntityTypeFullName = 'LegalRegTech.Authorization.Roles.Role'
    )

select
    base1.Tenant_Id,
    base1.Date_Time,
    base1.EventType,
    base1.Event,
    base2.Actioned_by,
    base1.Impacted + ' deleted' Changed,
    base1.Impacted

from base1
join base2 on base1.Tenant_Id = base2.Tenant_Id and base1.EntityId = base2.EntityId and base1.ChangeId = base2.ChangeId
