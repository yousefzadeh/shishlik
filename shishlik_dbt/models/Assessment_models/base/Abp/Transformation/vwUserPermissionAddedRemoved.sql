with
    base1 as (
        select
            cs.TenantId Tenant_Id,
            c.EntityId,
            c.ChangeTime Date_Time,
            'User Management' EventType,
            pc.NewValue IsGranted

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpRoles") }} ar on ar.Id = pc.NewValue
        -- where cs.TenantId = 1384
        where c.EntityTypeFullName = 'Abp.Authorization.Users.UserPermissionSetting' and pc.PropertyName = 'IsGranted'
    ),
    base2 as (
        select cs.TenantId Tenant_Id, c.EntityId, au.Name + ' ' + au.Surname Actioned_by

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = cs.UserId
        -- where cs.TenantId = 1384
        where c.EntityTypeFullName = 'Abp.Authorization.Users.UserPermissionSetting'

    ),
    base3 as (
        select cs.TenantId Tenant_Id, c.EntityId, au.UserName Impacted

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = pc.NewValue
        -- where cs.TenantId = 1384
        where c.EntityTypeFullName = 'Abp.Authorization.Users.UserPermissionSetting' and pc.PropertyName = 'UserId'
    ),
    base4 as (
        select cs.TenantId Tenant_Id, c.EntityId, pc.NewValue Permission

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        -- where cs.TenantId = 1384
        where c.EntityTypeFullName = 'Abp.Authorization.Users.UserPermissionSetting' and pc.PropertyNameVarChar = 'Name'
    )

select distinct
    case
        when base1.IsGranted = 'false' then base1.Tenant_Id when base1.IsGranted = 'true' then base1.Tenant_Id
    end Tenant_Id,
    case
        when base1.IsGranted = 'false' then base1.Date_Time when base1.IsGranted = 'true' then base1.Date_Time
    end Date_Time,
    base1.EventType,
    case
        when base1.IsGranted = 'false'
        then 'User Permission Removed'
        when base1.IsGranted = 'true'
        then 'User Permission Added'
    end Event,
    case
        when base1.IsGranted = 'false' then base2.Actioned_by when base1.IsGranted = 'true' then base2.Actioned_by
    end Actioned_by,
    case
        when base1.IsGranted = 'false'
        then base3.Impacted + ' was unassigned permission ' + base4.Permission
        when base1.IsGranted = 'true'
        then base3.Impacted + ' was assigned permission ' + base4.Permission
    end Changed,
    case
        when base1.IsGranted = 'false' then base3.Impacted when base1.IsGranted = 'true' then base3.Impacted
    end Impacted

from base1
left join base2 on base1.Tenant_Id = base2.Tenant_Id and base1.EntityId = base2.EntityId
left join base3 on base1.Tenant_Id = base3.Tenant_Id and base1.EntityId = base3.EntityId
left join
    base4 on base1.Tenant_Id = base4.Tenant_Id and base1.EntityId = base4.EntityId

    -- where base1.Tenant_Id = 1384
    
