with
    base1 as (
        select
            cs.tenantid tenant_id,
            c.entityid,
            cs.id[changeid],
            c.changetime date_time,
            'Group Management' eventtype,
            'Group Edited' event,
            -- case when pc.PropertyNameVarChar = 'IsDeleted' then pc.NewValue end
            -- GroupIsDeleted
            pc.newvalue groupisdeleted

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join
            {{ source("assessment_models", "AbpEntityChanges") }} c
            on cs.id = c.entitychangesetid
        left join
            {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc
            on c.id = pc.entitychangeid
        -- where cs.TenantId = 1384
        where
            c.entitytypefullname = 'Abp.Authorization.Users.UserOrganizationUnit'
            and pc.propertynamevarchar = 'IsDeleted'  -- and pc.NewValue = 'false'
    -- and case when pc.PropertyNameVarChar = 'IsDeleted' then pc.NewValue end is not
    -- null
    ),
    base2 as (
        select cs.tenantid tenant_id, c.entityid, au.name + ' ' + au.surname actioned_by

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join
            {{ source("assessment_models", "AbpEntityChanges") }} c
            on cs.id = c.entitychangesetid
        left join
            {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc
            on c.id = pc.entitychangeid
        left join
            {{ source("assessment_models", "AbpUsers") }} au on au.id = pc.newvalue
        -- where cs.TenantId = 1384
        where
            c.entitytypefullname = 'Abp.Authorization.Users.UserOrganizationUnit'
            and pc.propertynamevarchar = 'CreatorUserId'
    ),
    base4 as (
        select distinct
            cs.tenantid tenant_id,
            c.entityid,
            cs.id[changeid],
            au.name + ' ' + au.surname actioned_by

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join
            {{ source("assessment_models", "AbpEntityChanges") }} c
            on cs.id = c.entitychangesetid
        left join
            {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc
            on c.id = pc.entitychangeid
        left join
            {{ source("assessment_models", "AbpUserOrganizationUnits") }} auou
            on auou.id = c.entityid
        left join {{ source("assessment_models", "AbpUsers") }} au on au.id = cs.userid
        -- where cs.TenantId = 1384
        where
            c.entitytypefullname = 'Abp.Authorization.Users.UserOrganizationUnit'
            and pc.propertynamevarchar = 'IsDeleted'
    ),
    base3 as (
        select cs.tenantid tenant_id, c.entityid, aou.displayname impacted

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join
            {{ source("assessment_models", "AbpEntityChanges") }} c
            on cs.id = c.entitychangesetid
        left join
            {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc
            on c.id = pc.entitychangeid
        left join
            {{ source("assessment_models", "AbpOrganizationUnits") }} aou
            on aou.id = pc.newvalue
        -- where cs.TenantId = 1384
        where
            c.entitytypefullname = 'Abp.Authorization.Users.UserOrganizationUnit'
            and pc.propertynamevarchar = 'OrganizationUnitId'  -- commented out to test
    ),
    base6 as (
        select cs.tenantid tenant_id, c.entityid, aou.displayname impacted

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join
            {{ source("assessment_models", "AbpEntityChanges") }} c
            on cs.id = c.entitychangesetid
        left join
            {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc
            on c.id = pc.entitychangeid
        left join
            {{ source("assessment_models", "AbpUserOrganizationUnits") }} auou
            on auou.id = c.entityid
        left join
            {{ source("assessment_models", "AbpOrganizationUnits") }} aou
            on aou.id = auou.organizationunitid
        -- where cs.TenantId = 1384
        where
            c.entitytypefullname = 'Abp.Authorization.Users.UserOrganizationUnit'
            and pc.propertynamevarchar = 'IsDeleted'  -- commented out to test
    ),
    base5 as (
        select distinct
            cs.tenantid tenant_id, c.entityid, au.name + ' ' + au.surname users

        from {{ source("assessment_models", "AbpEntityChangeSets") }} cs
        left join
            {{ source("assessment_models", "AbpEntityChanges") }} c
            on cs.id = c.entitychangesetid
        left join
            {{ source("assessment_models", "AbpEntityPropertyChanges") }} pc
            on c.id = pc.entitychangeid
        left join
            {{ source("assessment_models", "AbpUserOrganizationUnits") }} auou
            on auou.id = c.entityid
        left join
            {{ source("assessment_models", "AbpUsers") }} au on au.id = auou.userid
        -- where cs.TenantId = 1384
        where c.entitytypefullname = 'Abp.Authorization.Users.UserOrganizationUnit'  -- and pc.PropertyNameVarChar = 'UserId'
    )

select distinct
    case
        when base1.groupisdeleted = 'false'
        then base1.tenant_id
        when base1.groupisdeleted = 'true'
        then base1.tenant_id
    end tenant_id,
    case
        when base1.groupisdeleted = 'false'
        then base1.date_time
        when base1.groupisdeleted = 'true'
        then base1.date_time
    end date_time,
    'Group Management' eventtype,
    event,
    case
        when base1.groupisdeleted = 'false'
        then base2.actioned_by
        when base1.groupisdeleted = 'true'
        then base4.actioned_by
    end actioned_by,
    case
        when base1.groupisdeleted = 'false'
        then base3.impacted + ' edited: ' + base5.users + ' added'
        when base1.groupisdeleted = 'true'
        then base6.impacted + ' edited: ' + base5.users + ' removed'
    end changed,
    case
        when base1.groupisdeleted = 'false'
        then base3.impacted
        when base1.groupisdeleted = 'true'
        then base6.impacted
    end impacted
from base1
left join base2 on base1.tenant_id = base2.tenant_id and base1.entityid = base2.entityid
left join base3 on base1.tenant_id = base3.tenant_id and base1.entityid = base3.entityid
left join base6 on base1.tenant_id = base6.tenant_id and base1.entityid = base6.entityid
left join
    base4
    on base1.tenant_id = base4.tenant_id
    and base1.entityid = base4.entityid
    and base1.changeid = base4.changeid
left join
    base5 on base1.tenant_id = base5.tenant_id and base1.entityid = base5.entityid

    -- where base1.Tenant_Id = 5777
    
