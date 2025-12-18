select
au.TenantId,
au.Authority_Id,
a.Assessment_Id,
a.Assessment_Name Authority_LinkedAssessments

from {{ ref("vAuthority") }} au
join {{ ref("vAssessment") }} a on a.Assessment_AuthorityId = au.Authority_Id