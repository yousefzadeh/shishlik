select *
from {{ ref("vwRoleCreated") }}

union all

select *
from {{ ref("vwRoleDeleted") }}

union all

select
    case
        when base1.GrantPermission = 'false'
        then base1.Tenant_Id
        when base1.GrantPermission = 'true'
        then base1.Tenant_Id
    end Tenant_Id,
    case
        when base1.GrantPermission = 'false'
        then base1.Date_Time
        when base1.GrantPermission = 'true'
        then base1.Date_Time
    end Date_Time,
    'Role Management' EventType,
    'Role Edited' Event,
    case
        when base1.GrantPermission = 'false'
        then base2.Actioned_by
        when base1.GrantPermission = 'true'
        then base2.Actioned_by
    end Actioned_by,
    case
        when base1.GrantPermission = 'false'
        then base4.Impacted + ' edited: permission ' + base3.Permission_Name + ' removed'
        when base1.GrantPermission = 'true'
        then base4.Impacted + ' edited: permission ' + base3.Permission_Name + ' added'
    end Changed,
    case
        when base1.GrantPermission = 'false' then base4.Impacted when base1.GrantPermission = 'true' then base4.Impacted
    end Impacted
from
    (
        select
            cs.TenantId Tenant_Id,
            c.EntityId,
            c.ChangeTime Date_Time,
            'Role Management' EventType,
            'Role Edited' Event,
            -- case when pc.PropertyNameVarChar = 'IsGranted' then pc.NewValue end GrantPermission
            pc.NewValue GrantPermission

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        -- where cs.TenantId = 1384
        where
            c.EntityTypeFullName = 'Abp.Authorization.Roles.RolePermissionSetting'
            and pc.PropertyNameVarChar = 'IsGranted'  -- and pc.NewValue = 'false'
    -- and case when pc.PropertyNameVarChar = 'IsGranted' then pc.NewValue end is not null
    ) base1
left join
    (
        select cs.TenantId Tenant_Id, c.EntityId, au.Name + ' ' + au.Surname Actioned_by

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = pc.NewValue
        -- where cs.TenantId = 1384
        where
            c.EntityTypeFullName = 'Abp.Authorization.Roles.RolePermissionSetting'
            and pc.PropertyNameVarChar = 'CreatorUserId'
    ) base2
    on base1.Tenant_Id = base2.Tenant_Id
    and base1.EntityId = base2.EntityId

left join
    (
        select cs.TenantId Tenant_Id, c.EntityId, pc.NewValue Permission_Name

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        -- where cs.TenantId = 1384
        where c.EntityTypeFullName = 'Abp.Authorization.Roles.RolePermissionSetting' and pc.PropertyNameVarChar = 'Name'
    ) base3
    on base1.Tenant_Id = base3.Tenant_Id
    and base1.EntityId = base3.EntityId

left join
    (
        select cs.TenantId Tenant_Id, c.EntityId, ar.DisplayName Impacted

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpRoles") }} ar on ar.Id = pc.NewValue
        -- where cs.TenantId = 1384
        where
            c.EntityTypeFullName = 'Abp.Authorization.Roles.RolePermissionSetting' and pc.PropertyNameVarChar = 'RoleId'
    ) base4
    on base1.Tenant_Id = base4.Tenant_Id
    and base1.EntityId = base4.EntityId

    -- where base1.Tenant_Id = 1384
    
