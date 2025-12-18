-- sources
with
    prov as (select ap.Id, ap.AuthorityId from {{ source("assessment_models", "AuthorityProvision") }} ap),
    ass as (
        select ass.Id, ass.AuthorityId, ass.PolicyId, ass.TenantId
        from {{ source("assessment_models", "Assessment") }} ass
    ),
    ac as (select AuthorityId, PolicyId from {{ source("assessment_models", "AuthorityPolicy") }}),
    mapping as (
        select m.SourceAuthorityId, m.TargetAuthorityId, TenantId
        from {{ source("tenant_models", "TenantAuthorityMapping") }} m
    ),
    -- -
    ass_unlinked as (
        select 'No Target Provision' part, 0 AuthorityProvision_Id, ass.Id Assessment_Id, ass.TenantId, 0 link_count
        from ass
        where AuthorityId is null and PolicyId is null
    ),
    target_prov_linked_to_ass as (
        select
            'Linked to Target Provision thru Source Provision' part,
            ap.Id AuthorityProvision_Id,
            ass.Id Assessment_Id,
            ass.TenantId,
            1 link_count
        from ass
        join mapping m on ass.AuthorityId = m.SourceAuthorityId and ass.TenantId = m.TenantId
        join prov ap on m.TargetAuthorityId = ap.AuthorityId
    ),
    target_prov_linked_to_ass_control as (
        select
            'Linked to Target Provision thru ControlSet' part,
            ap.Id AuthorityProvision_Id,
            ass.Id Assessment_Id,
            ass.TenantId,
            1 link_count
        from ass
        join ac on ass.PolicyId = ac.PolicyId
        join mapping m on ac.AuthorityId = m.SourceAuthorityId and ass.TenantId = m.TenantId
        join prov ap on m.TargetAuthorityId = ap.AuthorityId
    ),
    target_prov_linked as (
        select *
        from target_prov_linked_to_ass
        union all
        select *
        from target_prov_linked_to_ass_control
    ),
    target_auth_linked as (
        -- Assessments linked to Source Authority where there is a mapping to Target Authority
        select
            ass.AuthorityId SourceAuthorityId,  -- Authority linked directly
            m.TargetAuthorityId,  -- Related Target Authority
            ass.TenantId
        from ass
        join mapping m on ass.AuthorityId = m.SourceAuthorityId
        union all
        -- Assessments linked to Source Authority thru ControlSet where there is a mapping to Target Authority
        select
            ac.AuthorityId SourceAuthorityId,  -- Authority linked thru ControlSet
            m.TargetAuthorityId,  -- Related Target Autority
            ass.TenantId
        from ass
        join ac on ass.PolicyId = ac.PolicyId
        join mapping m on ac.AuthorityId = m.SourceAuthorityId
    ),
    target_prov_unlinked_id as (
        -- All Target provisions linked to Target Authority
        select distinct prov.Id AuthorityProvision_Id, target_auth_linked.TenantId
        from prov
        join target_auth_linked on prov.AuthorityId = target_auth_linked.TargetAuthorityId
        --
        except
        -- Target Provisions linked to questions
        select AuthorityProvision_Id, TenantId
        from target_prov_linked
    ),
    target_prov_unlinked as (
        select
            'Provision not Linked to Questions' part,
            AuthorityProvision_Id,
            0 Assessment_Id,
            TenantId,  -- need all Provisions at all Login Tenants
            0 link_count
        from target_prov_unlinked_id
    ),
    target_prov_zero as (
        select 'Unlinked Question' part, 0 AuthorityProvision_Id, Id Assessment_Id, TenantId, 0 link_count from ass
    ),
    ass_target_prov as (
        select *
        from target_prov_linked
        union all
        select *
        from target_prov_unlinked
        union all
        select *
        from ass_unlinked
    -- union all 
    -- select * from target_prov_zero
    )
select part, AuthorityProvision_Id TargetProvision_Id, Assessment_Id, TenantId Tenant_Id, link_count
from ass_target_prov
