	select  
	Policy_TenantId,
	Policy_Id,
	Policy_Name,
	PolicyDomain_Id,
	Controls_Id,
	Controls_Reference,
	Controls_Name 
	from {{ ref("vwPolicy") }} pol -- 6055 cs
	join {{ ref("vwPolicyDomain") }} pd on pd.PolicyDomain_PolicyId = pol.Policy_Id -- 16599 cs/domain
	join {{ ref("vwControls") }} c on pd.PolicyDomain_Id = c.Controls_PolicyDomainId -- 113533 cs/domain/control
