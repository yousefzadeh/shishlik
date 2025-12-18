with base as(
select *,case when ROW_NUMBER() over (
                partition by aula.AbpUserLoginAttempts_UserId order by aula.AbpUserLoginAttempts_TenantId, aula.AbpUserLoginAttempts_UserId, aula.AbpUserLoginAttempts_CreationTime, aula.AbpUserLoginAttempts_Id
            ) =1 then aula.AbpUserLoginAttempts_CreationTime end FirstLogin from {{ ref("vwAbpUserLoginAttempts") }} aula

where AbpUserLoginAttempts_Result = 1
            )
select * from base
where FirstLogin is not null