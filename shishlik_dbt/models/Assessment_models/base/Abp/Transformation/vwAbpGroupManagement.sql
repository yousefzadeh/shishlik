select
    case
        when base1.GroupIsDeleted = 'false' then base1.Tenant_Id when base1.GroupIsDeleted = 'true' then base1.Tenant_Id
    end Tenant_Id,
    case
        when base1.GroupIsDeleted = 'false' then base1.Date_Time when base1.GroupIsDeleted = 'true' then base1.Date_Time
    end Date_Time,
    'Group Management' EventType,
    case
        when base1.GroupIsDeleted = 'false' then 'Group Created' when base1.GroupIsDeleted = 'true' then 'Group Deleted'
    end Event,
    case
        when base1.GroupIsDeleted = 'false'
        then base2.Actioned_by
        when base1.GroupIsDeleted = 'true'
        then base4.Actioned_by
    end Actioned_by,
    case
        when base1.GroupIsDeleted = 'false'
        then base3.Impacted + ' created'
        when base1.GroupIsDeleted = 'true'
        then base3.Impacted + ' deleted'
    end Changed,
    case
        when base1.GroupIsDeleted = 'false' then base3.Impacted when base1.GroupIsDeleted = 'true' then base3.Impacted
    end Impacted
from
    (
        select
            cs.TenantId Tenant_Id,
            c.EntityId,
            c.ChangeTime Date_Time,
            'Group Management' EventType,
            -- case when pc.NewValue = 'false' then 'Group Created' 
            -- when pc.NewValue = 'true' then 'Group Deleted' end Event,
            pc.NewValue GroupIsDeleted

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        -- where cs.TenantId = 1384
        where c.EntityTypeFullName = 'Abp.Organizations.OrganizationUnit' and pc.PropertyNameVarChar = 'IsDeleted'  -- and pc.NewValue = 'false'
    ) base1

left join
    (
        select cs.TenantId Tenant_Id, c.EntityId, au.Name + ' ' + au.Surname Actioned_by

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = pc.NewValue
        -- where cs.TenantId = 1384
        where c.EntityTypeFullName = 'Abp.Organizations.OrganizationUnit' and pc.PropertyNameVarChar = 'CreatorUserId'
    ) base2
    on base1.Tenant_Id = base2.Tenant_Id
    and base1.EntityId = base2.EntityId

left join
    (
        select cs.TenantId Tenant_Id, c.EntityId, pc.NewValue Impacted

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        -- where cs.TenantId = 1384
        where c.EntityTypeFullName = 'Abp.Organizations.OrganizationUnit' and pc.PropertyNameVarChar = 'DisplayName'
    ) base3
    on base1.Tenant_Id = base3.Tenant_Id
    and base1.EntityId = base3.EntityId

left join
    (
        select cs.TenantId Tenant_Id, c.EntityId, au.Name + ' ' + au.Surname Actioned_by

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = pc.NewValue
        -- where cs.TenantId = 1384
        where c.EntityTypeFullName = 'Abp.Organizations.OrganizationUnit' and pc.PropertyNameVarChar = 'DeleterUserId'
    ) base4
    on base1.Tenant_Id = base4.Tenant_Id
    and base1.EntityId = base4.EntityId

-- where base1.Tenant_Id = 1384
union all

select
    base1.Tenant_Id,
    base1.Date_Time,
    base1.EventType,
    base1.Event,
    base2.Actioned_by,
    base3.Impacted + ' name edited' Changed,
    base3.Impacted Impacted
from
    (
        select
            cs.TenantId Tenant_Id,
            c.EntityId,
            cs.Id[ChangeId],
            c.ChangeTime Date_Time,
            'Group Management' EventType,
            'Group Edited' Event

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        -- where cs.TenantId = 1384
        where
            c.EntityTypeFullName = 'Abp.Organizations.OrganizationUnit'
            and pc.PropertyNameVarChar = 'LastModifierUserId'  -- and pc.NewValue = 'false'
            and c.ChangeType = 1
    ) base1

left join
    (
        select cs.TenantId Tenant_Id, c.EntityId, cs.Id[ChangeId], au.Name + ' ' + au.Surname Actioned_by

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = pc.NewValue
        -- where cs.TenantId = 1384
        where
            c.EntityTypeFullName = 'Abp.Organizations.OrganizationUnit'
            and pc.PropertyNameVarChar = 'LastModifierUserId'
            and c.ChangeType = 1
    ) base2
    on base1.Tenant_Id = base2.Tenant_Id
    and base1.EntityId = base2.EntityId
    and base1.ChangeId = base2.ChangeId

left join
    (
        select cs.TenantId Tenant_Id, c.EntityId, cs.Id[ChangeId], pc.NewValue Impacted

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join {{ source("assessment_models", "AbpEntityChanges") }} c on cs.Id = c.EntityChangeSetId
        left join {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc on c.Id = pc.EntityChangeId
        left join {{ source("assessment_models", "AbpOrganizationUnits") }} aou on aou.Id = pc.NewValue
        -- where cs.TenantId = 1384
        where
            c.EntityTypeFullName = 'Abp.Organizations.OrganizationUnit'
            and pc.PropertyNameVarChar = 'DisplayName'
            and c.ChangeType = 1
    ) base3
    on base1.Tenant_Id = base3.Tenant_Id
    and base1.EntityId = base3.EntityId
-- and base1.ChangeId = base3.ChangeId
-- where base1.Tenant_Id = 1384
union all

select Tenant_Id, Date_Time, EventType, Event, Actioned_by, Changed, Impacted
from {{ ref("vwAbpGroupAddedOrRemoved") }}
