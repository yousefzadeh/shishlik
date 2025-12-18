select distinct
    aal.TenantId Tenant_Id,
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
    'User Management' EventType,
    case
        when
            aal.MethodName = 'GetUIManagementSettingsAsync'
            and aal.ServiceName = 'LegalRegTech.Configuration.Tenants.TenantSettingsAppService'
        then 'Impersonation Start'
        when
            aal.MethodName = 'BackToImpersonator'
            and aal.ServiceName = 'LegalRegTech.Authorization.Accounts.AccountAppService'
        then 'Impersonation End'
    end Event,
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
    end Impacted

from {{ source("assessment_models", "AbpAuditLogs") }} aal
left join {{ source("assessment_models", "AbpUsers") }} au on au.Id = aal.ImpersonatorUserId
left join {{ source("assessment_models", "AbpUsers") }} au2 on au2.Id = aal.UserId
