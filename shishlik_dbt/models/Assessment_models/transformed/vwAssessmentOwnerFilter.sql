with
    owner_user as (
        select distinct ao.AssessmentOwner_AssessmentId, 
        ao.AssessmentOwner_UserId as Owner_Id, 
        u.AbpUsers_FullName OwnerName,
         case when u.AbpUsers_UpdateTime>=ao.AssessmentOwner_UpdateTime then u.AbpUsers_UpdateTime
			else ao.AssessmentOwner_UpdateTime 
            end as AssessmentOwner_UpdateTime --Picking the most recent date columns wrt AssessmentOwner and Abp updates
        from {{ ref("vwAssessmentOwner") }} ao
        join {{ ref("vwAbpUser") }} u on ao.AssessmentOwner_Userid = u.AbpUsers_Id
    ),
    owner_org as (
        select distinct ao.AssessmentOwner_AssessmentId, 
        ao.AssessmentOwner_OrganizationUnitId as Owner_Id, 
        o.AbpOrganizationUnits_DisplayName OwnerName,
        case when o.AbpOrganizationUnits_UpdateTime>=ao.AssessmentOwner_UpdateTime then o.AbpOrganizationUnits_UpdateTime 
			else ao.AssessmentOwner_UpdateTime 
            end as AssessmentOwner_UpdateTime --Picking the most recent date columns wrt AssessmentOwner and Abp updates
        from {{ ref("vwAssessmentOwner") }} ao
        join {{ ref("vwAbpOrganizationUnits") }} o on ao.AssessmentOwner_OrganizationUnitId = o.AbpOrganizationUnits_Id
    ),
    final as (
        Select distinct 
        [AssessmentOwner_AssessmentId]
			  ,[Owner_Id]
			  ,[OwnerName]
			  ,max(AssessmentOwner_UpdateTime)over( partition by [AssessmentOwner_AssessmentId],[Owner_Id],[OwnerName]) AssessmentOwner_UpdateTime
	    from (
        select [AssessmentOwner_AssessmentId]
			  ,[Owner_Id]
			  ,[OwnerName]
			  ,[AssessmentOwner_UpdateTime]
        from owner_user
        union all
        select [AssessmentOwner_AssessmentId]
			  ,[Owner_Id]
			  ,[OwnerName]
			  ,[AssessmentOwner_UpdateTime]
        from owner_org
        )a
    )

SELECT [AssessmentOwner_AssessmentId]
      ,[Owner_Id]
      ,[OwnerName]
      ,[AssessmentOwner_UpdateTime]
	  ,rank()over(order by AssessmentOwner_AssessmentId,Owner_id ) as AssessmentOwnerFilter_pk
  FROM final
