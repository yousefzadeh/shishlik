select * from {{ source("abp_ref_models", "AbpUsers") }} au
where au.IsDeleted = 0 and au.IsActive = 1