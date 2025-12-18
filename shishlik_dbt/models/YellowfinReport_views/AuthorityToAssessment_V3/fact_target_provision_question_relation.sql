with
    q as (select Id, AssessmentDomainId, TenantId from {{ source("assessment_models", "Question") }} q),
    ass_domain as (select Id, AssessmentId, TenantId from {{ source("assessment_models", "AssessmentDomain") }} ad),
    prov as (select ap.Id, ap.AuthorityId from {{ source("assessment_models", "AuthorityProvision") }} ap),
    mapping as (
        select SourceAuthorityProvisionId, TargetAuthorityProvisionId, TenantId
        from {{ source("tenant_models", "TenantAuthorityProvisionMapping") }}
    ),
    q_control_target_prov as (
        select 'Controls' part, cq.QuestionId, m.TargetAuthorityProvisionId, cq.TenantId
        from {{ source("assessment_models", "ControlQuestion") }} cq
        join {{ source("assessment_models", "ProvisionControl") }} pc on cq.ControlsId = pc.ControlsId
        join mapping m on pc.AuthorityReferenceId = m.SourceAuthorityProvisionId and cq.TenantId = m.TenantId
    ),
    q_target_prov as (
        select 'Provision' part, pq.QuestionId, m.TargetAuthorityProvisionId, pq.TenantId
        from {{ source("assessment_models", "ProvisionQuestion") }} pq
        join mapping m on pq.AuthorityProvisionId = m.SourceAuthorityProvisionId and pq.TenantId = m.TenantId
    ),
    -- --
    q_target_prov_linked as (
        select *
        from q_control_target_prov
        union all
        select *
        from q_target_prov
    ),
    q_target_prov_unlinked as (
        select 'Unlinked Question' part, q.Id Question_Id, 0 TargetAuthorityProvisionId, q.TenantId
        from q
        except
        select 'Unlinked Question' part, ql.QuestionId, 0 TargetAuthorityProvisionId, ql.TenantId
        from q_target_prov_linked ql
    ),
    q_target_prov_union as (
        select *
        from q_target_prov_linked
        union all
        select *
        from q_target_prov_unlinked
    )
select
    qsp.part,
    qsp.QuestionId Question_Id,
    qsp.TargetAuthorityProvisionId TargetProvision_Id,
    ad.AssessmentId Assessment_Id,
    qsp.TenantId Tenant_Id
from q_target_prov_union qsp
join q on qsp.QuestionId = q.Id
join ass_domain ad on q.AssessmentDomainId = ad.Id
