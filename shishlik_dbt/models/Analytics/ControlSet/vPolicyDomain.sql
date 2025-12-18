select
pd.Uuid,
pd.TenantId,
pd.Id PolicyDomain_Id,
pd.CreationTime PolicyDomain_CreationTime,
pd.Name PolicyDomain_Name,
pd.PolicyId ControlSet_Id

from PolicyDomain pd
where pd.IsDeleted = 0