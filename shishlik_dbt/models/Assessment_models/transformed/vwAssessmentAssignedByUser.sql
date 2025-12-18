/* Adding vwAbpUser.AbpUsers_EmailAddress column to the view - 04 june 2025*/
/* Fixed UpdateTime column to the view -  09 July 2025*/
/* Update code to use AssignedByUserId as well for string agg -  15 July 2025*/
with QuestionVendorUser as (
select distinct qvu.QuestionVendorUser_TenantId as Tenant_Id,
		qvu.QuestionVendorUser_AssessmentId as Assessment_Id,
        qvu.QuestionVendorUser_CreatorUserId as AssignedByUserId,
        coalesce(qvu.QuestionVendorUser_UserId, qvu.QuestionVendorUser_OrganizationUnitId) as AssignedId,
		qvu.QuestionVendorUser_UserId as AssignedToUserId,
		qvu.QuestionVendorUser_OrganizationUnitId as AssignedToOrgId, 
		max(qvu.QuestionVendorUser_UpdateTime) over (partition by qvu.QuestionVendorUser_TenantId,  qvu.QuestionVendorUser_AssessmentId,qvu.QuestionVendorUser_CreatorUserId) as UpdateTime
 from {{ ref("vwQuestionVendorUser") }} qvu
 --where QuestionVendorUser_AssessmentId  in ( 21245,10563, 24598)
 ),
    by_users as (
        select distinct
            qvu.Tenant_Id,
            qvu.Assessment_Id,
            qvu.AssignedByUserId,
            by_u.AbpUsers_FullName AssignedByUser,
            qvu.UpdateTime
        from QuestionVendorUser qvu
        left join {{ ref("vwAbpUser") }} by_u 
            on qvu.AssignedByUserId = by_u.AbpUsers_Id
    ),
    to_users as (
        select distinct
            qvu.Tenant_Id,
            qvu.Assessment_Id,
            qvu.AssignedToUserId,
            to_u.AbpUsers_FullName AssignedToUser,
            to_u.AbpUsers_EmailAddress AssignedToUserEmail,
            qvu.UpdateTime,
			'Indvidual' as AssignedUserType
        from QuestionVendorUser qvu
        left join {{ ref("vwAbpUser") }} to_u 
            on qvu.AssignedToUserId = to_u.AbpUsers_Id
    ),
    to_orgs as (
        select distinct
            qvu.Tenant_Id,
            qvu.Assessment_Id,
            qvu.AssignedToOrgId,
            to_org.AbpOrganizationUnits_DisplayName AssignedToOrg,
			'' as AssignedToUserEmail,
            qvu.UpdateTime,
			'Organization' as AssignedUserType
        from QuestionVendorUser qvu
         left join {{ ref("vwAbpOrganizationUnits") }} to_org
            on qvu.AssignedToOrgId = to_org.AbpOrganizationUnits_Id
    ),
    by_list as (
        select Tenant_Id,
            Assessment_Id,
			AssignedByUserId,
            AssignedByUser
        from by_users
    ),
	to_list as (
		select Tenant_Id,
			Assessment_Id,
			AssignedToUser,
			AssignedToUserId,
			AssignedUserType,
			case when PATINDEX('%@%', AssignedToUser) <> 0 then concat('(',trim(AssignedToUser),')') 
				when trim(AssignedToUserEmail) <>'' then concat(trim(AssignedToUser),' (', STRING_AGG(CONVERT(NVARCHAR(200), trim(AssignedToUserEmail)), ', '),')') 
				else trim(AssignedToUser)
				end as AssignedToUserEmailList,
				max(UpdateTime) as max_QuestionVendorUser_UpdateTime
			from
			(
				select *
				from to_users
				union all
				select *
				from to_orgs
			)a 
			group by Tenant_Id, Assessment_Id, AssignedToUser, AssignedToUserEmail, AssignedToUserId, AssignedUserType
	),
    final as (
		select qvu.Tenant_Id,
			qvu.Assessment_Id,
			by_list.AssignedByUser,
			to_list.AssignedUserType,
			string_agg(trim(to_list.AssignedToUser), ', ') AssignedToUserList,
			cast(string_agg(trim(to_list.AssignedToUserEmailList), ', ') as nvarchar(4000)) AssignedToUserEmailList,
			max(max_QuestionVendorUser_UpdateTime) as AssessmentAssignedByUser_UpdateTime
		from QuestionVendorUser qvu
		left join by_list
		on by_list.Tenant_Id = qvu.Tenant_Id 
		and by_list.Assessment_Id = qvu.Assessment_Id
		and by_list.AssignedByUserId = qvu.AssignedByUserId
		left join to_list 
		on qvu.Tenant_Id = to_list.Tenant_Id 
		and qvu.Assessment_Id = to_list.Assessment_Id
		and qvu.AssignedId = to_list.AssignedToUserId 
		where  to_list.AssignedToUserEmailList <> ' ()'  
	    group by qvu.Tenant_Id ,qvu.Assessment_Id, by_list.AssignedByUser, by_list.AssignedByUserId
            , to_list.AssignedUserType
),
main as(

SELECT [Tenant_Id]
      ,[Assessment_Id]
      ,[AssignedByUser]
      ,[AssignedToUserList]
      ,[AssignedToUserEmailList]
	  ,[AssignedUserType]
      ,[AssessmentAssignedByUser_UpdateTime]
	  , rank() OVER (ORDER BY [Tenant_Id],[Assessment_Id],[AssignedByUser],[AssignedToUserList],[AssignedToUserEmailList]) AS AssessmentAssignedByUser_pk
  FROM final 
  )

select * from main

