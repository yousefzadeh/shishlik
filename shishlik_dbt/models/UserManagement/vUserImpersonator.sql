select distinct
    aal.TenantId Tenant_Id,
    t.Name Tenant_Name,
    aal.Id,
    case
        when
            aal.MethodName = 'GetUIManagementSettingsAsync'
            and aal.ServiceName = 'LegalRegTech.Configuration.Tenants.TenantSettingsAppService'
        then aal.ExecutionTime
        when
            aal.MethodName = 'BackToImpersonator'
            and aal.ServiceName = 'LegalRegTech.Authorization.Accounts.AccountAppService'
        then aal.ExecutionTime
    end Date_Time,
    case
        when
            aal.MethodName = 'GetUIManagementSettingsAsync'
            and aal.ServiceName = 'LegalRegTech.Configuration.Tenants.TenantSettingsAppService'
        then 'Impersonation Start'
        when
            aal.MethodName = 'BackToImpersonator'
            and aal.ServiceName = 'LegalRegTech.Authorization.Accounts.AccountAppService'
        then 'Impersonation End'
    end [Event],
    case
        when
            aal.MethodName = 'GetUIManagementSettingsAsync'
            and aal.ServiceName = 'LegalRegTech.Configuration.Tenants.TenantSettingsAppService'
        then au.Name + ' ' + au.Surname
        when
            aal.MethodName = 'BackToImpersonator'
            and aal.ServiceName = 'LegalRegTech.Authorization.Accounts.AccountAppService'
        then au.Name + ' ' + au.Surname
    end Actioned_by,
    au.IsDeleted Actioned_By_IsDeleted,
    case
        when
            aal.MethodName = 'GetUIManagementSettingsAsync'
            and aal.ServiceName = 'LegalRegTech.Configuration.Tenants.TenantSettingsAppService'
        then au.Name + ' ' + au.Surname + ' impersonated ' + au2.Name + ' ' + au2.Surname
        when
            aal.MethodName = 'BackToImpersonator'
            and aal.ServiceName = 'LegalRegTech.Authorization.Accounts.AccountAppService'
        then au.Name + ' ' + au.Surname + ' ended impersonating ' + au2.Name + ' ' + au2.Surname
    end Changed,
    case
        when
            aal.MethodName = 'GetUIManagementSettingsAsync'
            and aal.ServiceName = 'LegalRegTech.Configuration.Tenants.TenantSettingsAppService'
        then au2.Name + ' ' + au2.Surname
        when
            aal.MethodName = 'BackToImpersonator'
            and aal.ServiceName = 'LegalRegTech.Authorization.Accounts.AccountAppService'
        then au2.Name + ' ' + au2.Surname
    end Impacted,
    au2.IsDeleted Impacted_IsDeleted,
    au2.Id EntityId

from {{ source("assessment_models", "AbpAuditLogs") }} aal
join {{ source("assessment_models", "AbpTenants") }} t on t.Id = aal.TenantId
left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = aal.ImpersonatorUserId
left join {{ source("assessment_models", "AbpUsers") }} au2 on au2.Id = aal.UserId
where case
        when
            aal.MethodName = 'GetUIManagementSettingsAsync'
            and aal.ServiceName = 'LegalRegTech.Configuration.Tenants.TenantSettingsAppService'
        then au.Name + ' ' + au.Surname
        when
            aal.MethodName = 'BackToImpersonator'
            and aal.ServiceName = 'LegalRegTech.Authorization.Accounts.AccountAppService'
        then au.Name + ' ' + au.Surname
    end is not null