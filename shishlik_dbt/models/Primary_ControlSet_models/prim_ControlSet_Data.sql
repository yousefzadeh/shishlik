with policies as(
select
p.TenantId,
p.Id Policy_Id,
p.IsDeleted,
p.CreationTime Policy_CreatedDate,
p.LastModificationTime Policy_LastUpdatedDate,
p.Name Policy_Name,
p.[Description] Policy_Description,
p.SupplierName Policy_SupplierName,
case
when p.Status = 1
then 'Edit'
when p.Status = 2
then 'Published'
when p.Status = 100
then 'Deprecated'
else 'Undefined'
end as Policy_Status,
p.LastReviewDate Policy_LastReviewDate,
p.NextReviewDate Policy_NextReviewDate,
p.IsHaileyDescription Policy_IsHaileyDescription,
tpa.ThirdPartyAttributes_Label Policy_Type,
case when lead([CreationTime]) over (partition by coalesce(RootPolicyId,[Id])  order by [Version]) is null then 1 else 0 end  IsCurrent
from {{ source("assessment_models", "Policy") }} p
left join {{ ref("vwPolicyCustomAttributeData") }} pcad on pcad.PolicyCustomAttributeData_PolicyId = p.Id
left join {{ ref("vwThirdPartyAttributes") }} tpa
on tpa.ThirdPartyAttributes_Id = pcad.PolicyCustomAttributeData_ThirdPartyAttributesId
left join {{ ref("vwThirdPartyControl") }} tpc on tpc.ThirdPartyControl_Id = tpa.ThirdPartyAttributes_ThirdPartyControlId and tpc.ThirdPartyControl_Type = 1 and tpc.ThirdPartyControl_Label = 'Type'

where p.IsDeleted = 0
and p.Status != 100
)
, policy_auth as (
select
p.TenantId, 
p.Policy_Id,
STRING_AGG(CONVERT(NVARCHAR(max), a.[Name]), ', ') LinkedAuthorityList
from policies p
join {{ source("assessment_models", "AuthorityPolicy") }} ap
on ap.PolicyId = p.Policy_Id and ap.IsDeleted = 0
join {{ source("assessment_models", "Authority") }} a
on a.Id = ap.AuthorityId and a.IsDeleted = 0
group by
p.TenantId,
p.Policy_Id
)
, policy_owner as(
select
psh.TenantId,
psh.PolicyId,
STRING_AGG(CONVERT(NVARCHAR(max), psh.StakeHolderName), ', ') Policy_Owner
from {{ source("assessment_models", "PolicyStakeHolders") }} psh
where psh.IsDeleted = 0 and psh.Role = 1
group by psh.TenantId,
psh.PolicyId, psh.Role
)
, policy_reviewer as(
select
psh.TenantId,
psh.PolicyId,
STRING_AGG(CONVERT(NVARCHAR(max), psh.StakeHolderName), ', ') Policy_Reviewer
from {{ source("assessment_models", "PolicyStakeHolders") }} psh
where psh.IsDeleted = 0 and psh.Role = 2
group by psh.TenantId,
psh.PolicyId, psh.Role
)
, policy_reader as(
select
psh.TenantId,
psh.PolicyId,
STRING_AGG(CONVERT(NVARCHAR(max), psh.StakeHolderName), ', ') Policy_Reader
from {{ source("assessment_models", "PolicyStakeHolders") }} psh
where psh.IsDeleted = 0 and psh.Role = 3
group by psh.TenantId,
psh.PolicyId, psh.Role
)
, policy_approver as(
select
psh.TenantId,
psh.PolicyId,
STRING_AGG(CONVERT(NVARCHAR(max), psh.StakeHolderName), ', ') Policy_Approver
from {{ source("assessment_models", "PolicyStakeHolders") }} psh
where psh.IsDeleted = 0 and psh.Role = 4
group by psh.TenantId,
psh.PolicyId, psh.Role
)

select
p.TenantId,
p.Policy_Id,
p.IsDeleted,
p.Policy_CreatedDate,
p.Policy_LastUpdatedDate,
p.Policy_Name,
p.Policy_Description,
p.Policy_SupplierName,
p.Policy_Status,
p.Policy_LastReviewDate,
p.Policy_NextReviewDate,
p.Policy_IsHaileyDescription,
Policy_Type,
LinkedAuthorityList,
Policy_Owner,
Policy_Reviewer,
Policy_Reader,
Policy_Approver,
concat('Control Set name and description: ', p.Policy_Name, ' / ', ' Its status is ', p.Policy_Status, '. This control set is linked to ', LinkedAuthorityList) Text

from policies p
left join policy_auth pauth
on pauth.TenantId = p.TenantId
and pauth.Policy_Id = p.Policy_Id
left join policy_owner po
on po.TenantId = p.TenantId
and po.PolicyId = p.Policy_Id
left join policy_reviewer pr
on pr.TenantId = p.TenantId
and pr.PolicyId = p.Policy_Id
left join policy_reader prd
on prd.TenantId = p.TenantId
and prd.PolicyId = p.Policy_Id
left join policy_approver pa
on pa.TenantId = p.TenantId
and pa.PolicyId = p.Policy_Id

where p.IsCurrent = 1