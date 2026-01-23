-- External table for Answer.csv (landing zone)
-- Column order matches CSV export order in /landing/Answer/
CREATE TABLE IF NOT EXISTS workspace.landing.answer (
  Id INT,
  CreationTime STRING,
  CreatorUserId BIGINT,
  LastModificationTime STRING,
  LastModifierUserId BIGINT,
  IsDeleted BOOLEAN,
  DeleterUserId BIGINT,
  DeletionTime STRING,
  TenantId INT,
  QuestionId INT,
  ComponentStr STRING,
  Status INT,
  AssessmentResponseId BIGINT,
  MaxPossibleScore DECIMAL(18,2),
  Score DECIMAL(18,2),
  RiskStatus INT,
  Compliance INT,
  ResponderId BIGINT,
  ReviewerComment STRING,
  HaileySuggestedAnswerStatus INT,
  Uuid STRING,
  HaileySuggestedComponentStr STRING
)
USING CSV
OPTIONS (
  header = 'false',
  nullValue = 'NULL',
  quote = '\"',
  escape = '\"',
  encoding = 'UTF-8',
  pathGlobFilter = '*.csv'
)
LOCATION 's3://databricks-sandbox-landing-560449670213/landing/Answer/';
