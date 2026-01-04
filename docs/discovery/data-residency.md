Principles to respect:
      - AU Gov requires different access rights to other regions
      - Data should not leave the region:
            - AU
            - AU-GOV
            - UK
            - US
            - US-GOV
            - JP
            - SG
            - UAE
            - CA
            - DE
      - We also have a 'dedicated instance' option, which requires "separation at the database level".
      Currently two paying customers for is option - could be more.
      E.g. ADMN, which is in the UAE region, but have a dedicated database.
      - Aggregate/anonymized data can potentially leave the region, but we tend to avoid
      - Data retention:
            - Ideally able to delete individual tenants (although this troublesome already)
      - Backup policy? May not be needed as long we can respect RTO of 1 day (e.g. built-in 7 day retention)
      - For dev/test/staging, there aren't any particular restrictions but we currently support:
            - Staging environment which hosts the upcoming release (usually 1 week in advance)
            - "Spinup" environments, which are branch-specific test environments that devs can create on demand.
            Except that currently only 2 fixed spinups support Yellowfin, and Yellowfin can't be created on demand.
      - Principle that dev/test and prod should be separate
      - Staging mimics production closely. Spinup is a bit different.
      - Two subscriptions currently for shared resources: shared-prod, shared-non-prod