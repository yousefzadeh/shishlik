with
    q as (select Id, AssessmentDomainId, TenantId from {{ source("assessment_models", "Question") }} q),
    ass_domain as (
        select Id, AssessmentId, [Name] Domain_Name, TenantId
        from {{ source("assessment_models", "AssessmentDomain") }} ad
        where IsDeleted = 0
    ),
    prov as (
        select ap.Id, ap.AuthorityId from {{ source("assessment_models", "AuthorityProvision") }} ap where IsDeleted = 0
    ),
    q_control_prov as (
        select 'Controls' part, cq.QuestionId, pc.AuthorityReferenceId AuthorityProvisionId, cq.TenantId, 1 link_count
        from {{ source("assessment_models", "ControlQuestion") }} cq
        join
            {{ source("assessment_models", "ProvisionControl") }} pc
            on cq.ControlsId = pc.ControlsId
            and cq.Tenantid = pc.TenantId
    ),
    q_prov as (
        select 'Provision' part, pq.QuestionId, pq.AuthorityProvisionId, pq.TenantId, 1 link_count
        from {{ source("assessment_models", "ProvisionQuestion") }} pq
        where IsDeleted = 0
    ),
    q_prov_linked as (
        select *
        from q_control_prov
        union all
        select *
        from q_prov
    ),
    q_prov_unlinked as (
        select 'Unlinked Question' part, q.Id Question_Id, 0 AuthorityProvisionId, q.TenantId, 0 link_count
        from q
        except
        select 'Unlinked Question' part, ql.QuestionId, 0 AuthorityProvisionId, ql.TenantId, 0 link_count
        from q_prov_linked ql
    ),
    q_source_prov as (
        select *
        from q_prov_linked
        union all
        select *
        from q_prov_unlinked
    ),
    final as (
        select
            qsp.part,
            qsp.TenantId Tenant_Id,
            qsp.AuthorityProvisionId Provision_Id,
            ad.AssessmentId Assessment_Id,
            ad.Domain_Name,
            qsp.QuestionId Question_Id,
            qsp.link_count
        from q_source_prov qsp
        join q on qsp.QuestionId = q.Id
        join ass_domain ad on q.AssessmentDomainId = ad.Id and ad.TenantId = qsp.TenantId
    )
select *
from final
