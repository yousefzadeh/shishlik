-- sources
with
    prov as (
        select ap.Id, ap.AuthorityId
        from {{ source("assessment_models", "AuthorityProvision") }} ap
        where ap.IsDeleted = 0
    ),
    ass as (
        select ass.Id, ass.AuthorityId, ass.PolicyId, ass.TenantId
        from {{ source("assessment_models", "Assessment") }} ass
        where
            ass.IsDeleted = 0
            and ass.IsArchived = 0
            and ass.Status in (4, 5, 6)
            and ass.IsDeprecatedAssessmentVersion = 0
    ),
    ac as (
        select AuthorityId, PolicyId, TenantId
        from {{ source("assessment_models", "AuthorityPolicy") }}
        where IsDeleted = 0
    ),
    -- Views
    -- Provisions linked to Assessment
    prov_linked_to_ass as (  -- linked thru provision
        select 'Provision' part, ap.Id AuthorityProvision_Id, ass.Id Assessment_Id, ass.TenantId, 1 link_count
        from ass inner hash
        join prov ap on ass.AuthorityId = ap.AuthorityId
    ),
    prov_linked_to_ass_control as (  -- linked thru controlset-controls-provision
        select 'ControlSet' part, ap.Id AuthorityProvision_Id, ass.Id Assessment_Id, ass.TenantId, 1 link_count
        from ass inner hash
        join ac on ass.PolicyId = ac.PolicyId and ass.TenantId = ac.TenantId inner hash
        join prov ap on ac.AuthorityId = ap.AuthorityId
    ),
    prov_linked as (
        select *
        from prov_linked_to_ass
        union all
        select *
        from prov_linked_to_ass_control
    ),
    -- Assessments with no links to provision
    ass_unlinked as (
        select 'Unlinked Assessment' part, 0 AuthorityProvision_Id, ass.Id Assessment_Id, ass.TenantId, 0 link_count
        from ass
        where AUthorityId is null and PolicyId is null
    ),
    auth_linked as (
        select ass.AuthorityId, ass.TenantId
        from ass
        where AuthorityId is not NULL
        union all
        select ac.AuthorityId, ass.TenantId
        from ass inner hash
        join ac on ass.PolicyId = ac.PolicyId and ass.TenantId = ac.TenantId
    ),
    prov_unlinked_id as (
        select distinct prov.Id AuthorityProvision_Id, auth_linked.TenantId
        from prov inner hash
        join auth_linked on prov.AuthorityId = auth_linked.AuthorityId
        except
        select AuthorityProvision_Id, TenantId
        from prov_linked
    ),
    prov_unlinked as (
        select
            'Unlinked Provision' part,
            AuthorityProvision_Id,
            0 Assessment_Id,
            TenantId,  -- need all Provisions at all Login Tenants
            0 link_count
        from prov_unlinked_id
    ),
    ass_source_prov as (
        select *
        from prov_linked
        union all
        select *
        from prov_unlinked
        union all
        select *
        from ass_unlinked
    ),
    final as (
        select part, AuthorityProvision_Id Provision_Id, Assessment_Id, TenantId Tenant_Id, link_count
        from ass_source_prov
    )
select *
from final
