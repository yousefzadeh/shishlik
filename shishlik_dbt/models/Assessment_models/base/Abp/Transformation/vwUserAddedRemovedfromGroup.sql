with
    base1 as (
        select
            cs.TenantId Tenant_Id,
            c.EntityId,
            c.ChangeTime Date_Time,
            'User Management' EventType,
            pc.NewValue IsDeleted

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpRoles") }} ar on ar.Id = pc.NewValue
        -- where cs.TenantId = 1384
        where
            c.EntityTypeFullName = 'Abp.Authorization.Users.UserOrganizationUnit'
            and pc.PropertyNameVarChar = 'IsDeleted'  -- and pc.NewValue = 'false'
    ),
    base2 as (
        select cs.TenantId Tenant_Id, c.EntityId, au.Name + ' ' + au.Surname Actioned_by

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = pc.NewValue
        -- where cs.TenantId = 1384
        where
            c.EntityTypeFullName = 'Abp.Authorization.Users.UserOrganizationUnit'
            and pc.PropertyNameVarChar = 'CreatorUserId'
    ),
    base3 as (
        select cs.TenantId Tenant_Id, c.EntityId, au.Name + ' ' + au.Surname Actioned_by

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = cs.UserId
        -- where cs.TenantId = 1384
        where
            c.EntityTypeFullName = 'Abp.Authorization.Users.UserOrganizationUnit'
            and pc.PropertyNameVarChar = 'IsDeleted'
            and pc.NewValue = 'true'
    ),
    base4 as (
        select cs.TenantId Tenant_Id, c.EntityId, au.UserName Impacted

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = pc.NewValue
        -- where cs.TenantId = 1384
        where
            c.EntityTypeFullName = 'Abp.Authorization.Users.UserOrganizationUnit' and pc.PropertyNameVarChar = 'UserId'
    ),
    base5 as (
        select cs.TenantId Tenant_Id, c.EntityId, aou.DisplayName Groups

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpOrganizationUnits") }} aou on aou.Id = pc.NewValue
        -- where cs.TenantId = 1384
        where
            c.EntityTypeFullName = 'Abp.Authorization.Users.UserOrganizationUnit'
            and pc.PropertyNameVarChar = 'OrganizationUnitId'
    ),
    base6 as (
        select cs.TenantId Tenant_Id, c.EntityId, aou.DisplayName Groups, au.UserName Impacted

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpUserOrganizationUnits") }} auou on auou.Id = c.EntityId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = auou.UserId
        left join {{ source("assessment_models", "AbpOrganizationUnits") }} aou on aou.Id = auou.OrganizationUnitId
        -- where cs.TenantId = 1384
        where c.EntityTypeFullName = 'Abp.Authorization.Users.UserOrganizationUnit'
    )

select distinct
    case
        when base1.IsDeleted = 'false' then base1.Tenant_Id when base1.IsDeleted = 'true' then base1.Tenant_Id
    end Tenant_Id,
    case
        when base1.IsDeleted = 'false' then base1.Date_Time when base1.IsDeleted = 'true' then base1.Date_Time
    end Date_Time,
    base1.EventType,
    case
        when base1.IsDeleted = 'false'
        then 'User Added to Group'
        when base1.IsDeleted = 'true'
        then 'User Removed from Group'
    end Event,
    case
        when base1.IsDeleted = 'false' then base2.Actioned_by when base1.IsDeleted = 'true' then base3.Actioned_by
    end Actioned_by,
    case
        when base1.IsDeleted = 'false'
        then base4.Impacted + ' added to group ' + base5.Groups
        when base1.IsDeleted = 'true'
        then base6.Impacted + ' removed from group ' + base6.Groups
    end Changed,
    case
        when base1.IsDeleted = 'false' then base4.Impacted when base1.IsDeleted = 'true' then base6.Impacted
    end Impacted

from base1
left join base2 on base1.Tenant_Id = base2.Tenant_Id and base1.EntityId = base2.EntityId
left join base3 on base1.Tenant_Id = base3.Tenant_Id and base1.EntityId = base3.EntityId
left join base4 on base1.Tenant_Id = base4.Tenant_Id and base1.EntityId = base4.EntityId
left join base5 on base1.Tenant_Id = base5.Tenant_Id and base1.EntityId = base5.EntityId
left join
    base6 on base1.Tenant_Id = base6.Tenant_Id and base1.EntityId = base6.EntityId

    -- where base1.Tenant_Id = 1384
    
