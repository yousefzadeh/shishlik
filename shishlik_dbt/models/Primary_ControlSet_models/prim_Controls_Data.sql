with ctrl_set as (select *, 
case when lead(p.CreationTime) over (partition by coalesce(p.RootPolicyId,p.Id)  order by p.Version) is null then 1 else 0 end IsCurrent 
from {{ source("assessment_models", "Policy") }} p),

policies as(
    select
    p.TenantId,
    p.Id Policy_Id,
    cast(c.IsDeleted as int) IsDeleted,
    p.Name Policy_Name,
    pd.Name PolicyDomain_Name,
    c.Id Controls_Id,
    c.Reference Controls_RefId,
    c.Name Controls_Name,
    dbo.udf_StripHTML(c.Detail) Controls_Description,
    c.CreationTime Controls_CreatedDate,
    COALESCE(c.LastModificationTime, c.CreationTime) Controls_LastUpdatedDate
    
    from ctrl_set p
    join {{ source("assessment_models", "PolicyDomain") }} pd
    on pd.PolicyId = p.Id and pd.TenantId = p.TenantId and pd.IsDeleted = 0
    join {{ source("assessment_models", "Controls") }} c
    on c.PolicyDomainId = pd.Id and c.TenantId = pd.TenantId

    where p.IsDeleted = 0 and p.Status != 100 and p.IsCurrent = 1
), 
ctrl_prov_uni as (
    select distinct
    p.TenantId,
    p.Controls_Id,
    ap.ReferenceId
    
    from policies p
    join {{ source("assessment_models", "ProvisionControl") }} pc
    on pc.ControlsId = p.Controls_Id and pc.IsDeleted = 0
    join {{ source("assessment_models", "AuthorityProvision") }} ap
    on ap.Id = pc.AuthorityReferenceId and ap.IsDeleted = 0
), 
ctrl_prov as (
    select distinct
    ap.TenantId,
    ap.Controls_Id,
    STRING_AGG(CONVERT(NVARCHAR(max), ap.ReferenceId), ', ') LinkedAuthorityProvisionRefIdList
    
    from ctrl_prov_uni ap
    group by
    ap.TenantId,
    ap.Controls_Id
),
responsibilities as (
    select 
    p.TenantId,
    p.Controls_Id,
    STRING_AGG(CONVERT(NVARCHAR(max), s.Title), ', ') LinkedResponsibilityList
    from policies p
    join {{ source("statement_models", "StatementControl") }} sc
    on sc.TenantId = p.TenantId
    and sc.ControlId = p.Controls_Id and sc.IsDeleted = 0
    join {{ source("statement_models", "Statement") }} s
    on s.Id = sc.StatementId and s.IsDeleted = 0
    group by
    p.TenantId,
    p.Controls_Id
)

select
p.TenantId,
p.Policy_Id,
p.IsDeleted,
p.Policy_Name,
p.PolicyDomain_Name,
p.Controls_Id,
p.Controls_RefId,
p.Controls_Name,
p.Controls_Description,
p.Controls_CreatedDate,
p.Controls_LastUpdatedDate,
cp.LinkedAuthorityProvisionRefIdList,
r.LinkedResponsibilityList,
concat('Control name and description: ', p.Controls_Name, '. This control is linked to ', cp.LinkedAuthorityProvisionRefIdList, ' and ', r.LinkedResponsibilityList, '.') Text

from policies p
left join ctrl_prov cp
on cp.TenantId = p.TenantId and cp.Controls_Id = p.Controls_Id
left join responsibilities r
on r.TenantId = p.TenantId and r.Controls_Id = p.Controls_Id