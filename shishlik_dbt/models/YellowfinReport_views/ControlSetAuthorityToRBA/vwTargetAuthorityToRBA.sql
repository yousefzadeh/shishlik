with
    authority as (
        select a.Id Authority_Id, a.TenantId Tenant_Id, a.Name Authority_Name
        -- Coalesce(LastModificationTime,CreationTime) Authority_UpdateTime
        from {{ source("assessment_models", "Authority") }} a
        where IsDeleted = 0
    ),
    authorityprovision as (
        select
            Id AuthorityProvision_Id,
            AuthorityId AuthorityProvision_AuthorityId,
            ReferenceId AuthorityProvision_ReferenceId,
            Name AuthorityProvision_Name,
            TenantId Tenant_Id
        from {{ source("assessment_models", "AuthorityProvision") }}
        where IsDeleted = 0
    ),
    targetprovision as (
        select
            src.authorityprovision_id,
            src.authorityprovision_authorityid,
            tgt.authorityprovision_id targetprovision_id,
            tgt.authorityprovision_authorityid targetprovision_authorityid,
            tgt.authorityprovision_referenceid targetprovision_referenceid,
            tgt.authorityprovision_name targetprovision_name,
            map.tenantid
        from authorityprovision src
        join
            {{ source("tenant_models", "TenantAuthorityProvisionMapping") }} map
            on src.authorityprovision_id = map.sourceauthorityprovisionid
        join
            authorityprovision tgt
            on tgt.authorityprovision_id = map.targetauthorityprovisionid
        where map.isdeleted = 0
    ),
    assessment_with_template as (
        select
            ass.id assessment_id,
            ass.namevarchar assessment_name,
            tpl.namevarchar + case
                when tpl.isdeleted = 1
                then ' (Deleted)'
                when tpl.isarchived = 1
                then ' (Archived)'
                else ''
            end template_name,
            ass.authorityid assessment_authorityid,
            ass.tenantid assessment_tenantid
        from {{ source("assessment_models", "Assessment") }} as ass
        join
            {{ source("assessment_models", "Assessment") }} as tpl
            on ass.createdfromtemplateid = tpl.id
        where
            ass.istemplate = 0
            and ass.isdeleted = 0
            and ass.isarchived = 0
            and ass.isdeprecatedassessmentversion = 0
            and ass.status in (4, 5, 6)
            and ass.workflowid = 1
            and ass.authorityid is not null
    ),
    assessment_without_template as (
        select
            ass.id assessment_id,
            ass.namevarchar assessment_name,
            'No Template' template_name,
            ass.authorityid assessment_authorityid,
            ass.tenantid assessment_tenantid
        from {{ source("assessment_models", "Assessment") }} as ass
        where
            ass.istemplate = 0
            and ass.isdeleted = 0
            and ass.isarchived = 0
            and ass.isdeprecatedassessmentversion = 0
            and ass.status in (4, 5, 6)
            and ass.workflowid = 1
            and ass.createdfromtemplateid is null
            and ass.authorityid is not null
    ),
    assessment as (
        select *
        from assessment_with_template
        union all
        select *
        from assessment_without_template
    ),
    assessmentdomain as (
        select
            id assessmentdomain_id,
            assessmentid assessmentdomain_assessmentid,
            name assessmentdomain_name,
            tenantid
        from {{ source("assessment_models", "AssessmentDomain") }}
    ),
    assessmentresponse as (
        select
            id assessmentresponse_id,
            assessmentid assessmentresponse_assessmentid,
            tenantid
        from {{ source("assessment_models", "AssessmentResponse") }}
    ),
    assessmentdomainprovision_selected as (
        select
            id assessmentdomainprovision_id,
            authorityprovisionid assessmentdomainprovision_authorityprovisionid,
            assessmentdomainid assessmentdomainprovision_assessmentdomainid,
            'Selected' assessmentdomainprovision_part,
            tenantid
        from {{ source("assessment_models", "AssessmentDomainProvision") }}
        where isdeleted = 0
    ),
    assessmentdomainprovision_excluded as (
        select
            id assessmentdomainprovision_id,
            authorityprovisionid assessmentdomainprovision_authorityprovisionid,
            assessmentdomainid assessmentdomainprovision_assessmentdomainid,
            'Excluded' assessmentdomainprovision_part,
            tenantid
        from {{ source("assessment_models", "AssessmentDomainProvision") }}
        where isdeleted = 1
    ),
    assessmentcustomfield as (
        select
            assessmentid assessmentcustomfield_assessmentid,
            customfieldid assessmentcustomfield_customfieldid,
            tenantid
        from {{ source("assessment_models", "AssessmentCustomField") }}
    ),
    customfield as (
        select id customfield_id, name customfield_name, tenantid
        from {{ source("assessment_models", "CustomField") }}
    ),
    assessmentdomainprovisionresponsedata as (
        select
            assessmentdomainprovisionresponsedata_id,
            assessmentdomainprovisionresponsedata_assessmentresponseid,
            assessmentdomainprovisionresponsedata_assessmentdomainprovisionid,
            assessmentdomainprovisionresponsedata_customfieldresponse,
            assessmentdomainprovisionresponsedata_customfieldid,
            1 assessmentdomainprovisionresponsedata_isresponded,
            0 assessmentdomainprovisionresponsedata_isdeleted,
            assessmentdomainprovisionresponsedata_tenantid tenant_id
        from {{ ref("vwAssessmentDomainProvisionResponseData") }}
    ),
    prov_custom as (
        select
            Tenant_Id,
            authorityprovisioncustomvalue_authorityid,
            authorityprovisioncustomvalue_authorityprovisionid,
            authorityprovisioncustomvalue_fieldname,
            authorityprovisioncustomvalue_value,
            authorityprovisioncustomvalue_fieldtype
        from {{ ref("vwAuthorityProvisionCustomValue") }}
    ),
    source_target_authority_assessment as (
        select
            -- Filter
            tap.tenantid filter_tenantid,
            --ass.assessment_tenantid filter_tenantid,
            auth.authority_name filter_source_authority,
            target_auth.authority_name filter_target_authority,
            ass.template_name filter_template_name,
            ass.assessment_name filter_assessment_name,
            cf.customfield_name filter_assessment_field_name,
            -- Section header
            prov_custom.authorityprovisioncustomvalue_fieldname section_authority_field_name,
            -- Table
            ass.assessment_name assessment_name,
            ad.assessmentdomain_name assessmentdomain_name,
            tap.targetprovision_referenceid provision_idref,
            tap.targetprovision_name provision_name,
            case
                when
                    prov_custom.authorityprovisioncustomvalue_value is null
                    or prov_custom.authorityprovisioncustomvalue_value = ''
                then 'Unassigned'
                else prov_custom.authorityprovisioncustomvalue_value
            end as authority_field_value,
            prov_custom.authorityprovisioncustomvalue_fieldtype customfieldtype,
            -- Join Columns
            tap.authorityprovision_id,
            cf.customfield_id,
            ad.assessmentdomain_id,
            ass.assessment_id
        --
        -- Table Joins
        --
        -- Authority 
        -- Source Authority join Source Provision
        from authority as auth
        -- Provision to Provision Custom Field Value
        inner join
            targetprovision as tap
            on auth.authority_id = tap.authorityprovision_authorityid
        -- Target Authority and Custom Value
        inner join
            authority as target_auth
            on tap.targetprovision_authorityid = target_auth.authority_id
        left outer join
            prov_custom
            on tap.targetprovision_id
            = prov_custom.authorityprovisioncustomvalue_authorityprovisionid
            -- and tap.authorityprovision_authorityid = prov_custom.authorityprovisioncustomvalue_authorityid
            and tap.TenantId = prov_custom.Tenant_Id
        -- Assessment Template 
        inner join
            assessment as ass
            on ass.assessment_authorityid = auth.authority_id
            and ass.assessment_tenantid = tap.tenantid
        -- Assessment Custom Field Value
        inner join
            assessmentcustomfield as acf
            on ass.assessment_id = acf.assessmentcustomfield_assessmentid
            and ass.assessment_tenantid = acf.tenantid
        inner join
            customfield as cf
            on acf.assessmentcustomfield_customfieldid = cf.customfield_id
            and acf.tenantid = cf.tenantid
        -- Assessment Domain 
        inner hash join
            assessmentdomain as ad
            on ass.assessment_id = ad.assessmentdomain_assessmentid
            and ass.assessment_tenantid = ad.tenantid
    ),
    -- --
    selected_requirement_responded as (
        select
            'Selected requirement and responded' part,
            -- Filter
            staa.filter_tenantid,
            staa.filter_source_authority,
            staa.filter_target_authority,
            staa.filter_template_name,
            staa.filter_assessment_name,
            staa.filter_assessment_field_name,
            -- Section header
            staa.section_authority_field_name,
            -- Table
            staa.assessment_name,
            staa.assessmentdomain_name,
            staa.provision_idref,
            staa.provision_name,
            staa.authority_field_value,
            staa.customfieldtype,
            resp.assessmentdomainprovisionresponsedata_customfieldresponse actual_response,
            resp.assessmentdomainprovisionresponsedata_id response_id,
            -- Internal use
            staa.authorityprovision_id,
            ar.assessmentresponse_id
        --
        -- Table Joins
        --
        -- Authority 
        -- Source Authority join Source Provision
        from source_target_authority_assessment staa
        inner hash join
            assessmentdomainprovision_selected as req
            on staa.authorityprovision_id
            = req.assessmentdomainprovision_authorityprovisionid
            and staa.assessmentdomain_id
            = req.assessmentdomainprovision_assessmentdomainid
            and staa.filter_tenantid = req.tenantid
        inner hash join
            assessmentresponse as ar
            on staa.assessment_id = ar.assessmentresponse_assessmentid
            and staa.filter_tenantid = ar.tenantid
        inner hash join
            assessmentdomainprovisionresponsedata as resp
            on staa.customfield_id
            = resp.assessmentdomainprovisionresponsedata_customfieldid
            and ar.tenantid = resp.tenant_id
            and ar.assessmentresponse_id
            = resp.assessmentdomainprovisionresponsedata_assessmentresponseid
            and req.tenantid = resp.tenant_id
            and req.assessmentdomainprovision_id
            = resp.assessmentdomainprovisionresponsedata_assessmentdomainprovisionid
            and staa.filter_tenantid = resp.tenant_id
    ),
    selected_requirement_not_responded as (
        select
            'Selected requirement but not responded' part,
            -- Filter
            staa.filter_tenantid,
            staa.filter_source_authority,
            staa.filter_target_authority,
            staa.filter_template_name,
            staa.filter_assessment_name,
            staa.filter_assessment_field_name,
            -- Section header
            staa.section_authority_field_name,
            -- Table
            staa.assessment_name,
            staa.assessmentdomain_name,
            staa.provision_idref,
            staa.provision_name,
            staa.authority_field_value,
            staa.customfieldtype,
            'No Response' actual_response,
            assessmentdomainprovision_id response_id,
            -- Internal use
            staa.authorityprovision_id,
            0 assessmentresponse_id
        --
        -- Table Joins
        --
        -- Authority 
        -- Source Authority join Source Provision
        from source_target_authority_assessment staa
        inner join
            assessmentdomainprovision_selected as req
            on staa.authorityprovision_id = req.assessmentdomainprovision_authorityprovisionid
            and staa.assessmentdomain_id = req.assessmentdomainprovision_assessmentdomainid
            and staa.filter_tenantid = req.tenantid
        where
            not exists (
                select 1
                from selected_requirement_responded srr
                where srr.authorityprovision_id = staa.authorityprovision_id
            )
    ),
    excluded_requirement as (
        select
            'Excluded requirement' part,
            -- Filter
            staa.filter_tenantid,
            staa.filter_source_authority,
            staa.filter_target_authority,
            staa.filter_template_name,
            staa.filter_assessment_name,
            staa.filter_assessment_field_name,
            -- Section header
            staa.section_authority_field_name,
            -- Table
            staa.assessment_name,
            staa.assessmentdomain_name,
            staa.provision_idref,
            staa.provision_name,
            staa.authority_field_value,
            staa.customfieldtype,
            'Requirement excluded from assessment' actual_response,
            req.assessmentdomainprovision_id response_id,  -- Unique ID of requirement
            -- Internal use
            staa.authorityprovision_id,
            0 assessmentresponse_id
        --
        -- Table Joins
        --
        from source_target_authority_assessment staa
        -- Authority 
        inner join
        -- Source Authority join Source Provision
            assessmentdomainprovision_excluded as req
            on staa.authorityprovision_id = req.assessmentdomainprovision_authorityprovisionid
            and staa.assessmentdomain_id = req.assessmentdomainprovision_assessmentdomainid
            and staa.filter_tenantid = req.tenantid
    ),
    -- -----
    final as (
        select *
        from selected_requirement_responded
        union all
        select *
        from selected_requirement_not_responded
        union all
        select *
        from excluded_requirement
    )
select *
from final
