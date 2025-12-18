with
    control_list as (
        select q.TenantId, q.Id, string_agg(c.Reference + ' ' + c.Name, ', ') ProvisionControlList
		, max(coalesce(cq.LastModificationTime,cq.CreationTime)) LastModificationTime
        , count(*) n
        from {{ source("assessment_models","Question") }} q
        join {{ source("assessment_models","ControlQuestion") }} cq on q.Id = cq.QuestionId
        join {{ source("assessment_models","Controls") }} c on cq.ControlsId = c.Id
        group by q.TenantId, q.Id
    ),
    provision_list as (
        select q.TenantId, q.Id, string_agg(ap.ReferenceId + ' ' + ap.Name, ', ') ProvisionControlList
		, max(coalesce(pq.LastModificationTime,pq.CreationTime)) LastModificationTime
         , count(*) n
        from {{ source("assessment_models","Question") }} q
        join {{ source("assessment_models","ProvisionQuestion") }} pq on q.Id = pq.QuestionId
        join {{ source("assessment_models","AuthorityProvision") }} ap on pq.AuthorityProvisionId = ap.Id
        group by q.TenantId, q.Id
    ),
    all_list as (
        select *
        from control_list
        union all
        select *
        from provision_list
    ),
    final as (
        select 
        Id,
        ProvisionControlList,
        TenantId Tenant_Id,
        Id Question_Id,
	    LastModificationTime as QPC_UpdateTime
        from all_list 
    )
select 
Id,
ProvisionControlList,
Tenant_Id,
Question_Id,
QPC_UpdateTime
from final
{# where Tenant_Id = 1384 #}
