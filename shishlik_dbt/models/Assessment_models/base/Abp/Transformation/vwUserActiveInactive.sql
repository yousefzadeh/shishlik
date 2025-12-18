with
    base1 as (
        select distinct
            cs.TenantId Tenant_Id,
            c.EntityId,
            cs.Id ChangeId,
            c.ChangeTime Date_Time,
            'User Management' EventType,
            pc.NewValue IsActive

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        -- where cs.TenantId = 7
        where c.EntityTypeFullName = 'LegalRegTech.Authorization.Users.User' and pc.PropertyNameVarChar = 'IsActive'  -- and pc.NewValue = 'false'

    ),
    base2 as (
        select distinct cs.TenantId Tenant_Id, c.EntityId, cs.Id ChangeId, au.Name + ' ' + au.Surname Actioned_by

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = cs.UserId
        -- where cs.TenantId = 1384
        where c.EntityTypeFullName = 'LegalRegTech.Authorization.Users.User'

    ),
    base3 as (
        select distinct cs.TenantId Tenant_Id, c.EntityId, cs.Id ChangeId, au.UserName Impacted

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = c.EntityId
        -- where cs.TenantId = 1384
        where c.EntityTypeFullName = 'LegalRegTech.Authorization.Users.User'
    -- and pc.PropertyNameVarChar = 'Name'
    )

select distinct
    case
        when base1.IsActive = 'false' then base1.Tenant_Id when base1.IsActive = 'true' then base1.Tenant_Id
    end Tenant_Id,
    case
        when base1.IsActive = 'false' then base1.Date_Time when base1.IsActive = 'true' then base1.Date_Time
    end Date_Time,
    'User Management' EventType,
    case
        when base1.IsActive = 'false' then 'User Made Inactive' when base1.IsActive = 'true' then 'User Made Active'
    end Event,
    case
        when base1.IsActive = 'false' then base2.Actioned_by when base1.IsActive = 'true' then base2.Actioned_by
    end Actioned_by,
    case
        when base1.IsActive = 'false'
        then base3.Impacted + ' made inactive'
        when base1.IsActive = 'true'
        then base3.Impacted + ' made active'
    end Changed,
    case when base1.IsActive = 'false' then base3.Impacted when base1.IsActive = 'true' then base3.Impacted end Impacted
from base1
left join
    base2 on base1.Tenant_Id = base2.Tenant_Id and base1.EntityId = base2.EntityId and base1.ChangeId = base2.ChangeId
left join
    base3 on base1.Tenant_Id = base3.Tenant_Id and base1.EntityId = base3.EntityId and base1.ChangeId = base2.ChangeId

    -- where base1.Tenant_Id = 7
    
