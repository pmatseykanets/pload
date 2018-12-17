CREATE SCHEMA IF NOT EXISTS marketo;

CREATE TABLE IF NOT EXISTS marketo.activities (
    marketoGUID BIGINT NOT NULL,
    leadId INT,
    activityDate TIMESTAMP,
    activityTypeId INT,
    campaignId INT,
    primaryAttributeValueId INT,
    primaryAttributeValue CITEXT,
    attributes JSONB
);

CREATE UNIQUE INDEX IF NOT EXISTS activities_marketoguid_idx
    ON marketo.activities (marketoGUID);
CREATE INDEX IF NOT EXISTS activities_leadid_idx
    ON marketo.activities (leadId);
CREATE INDEX IF NOT EXISTS activities_activitydate_activitytypeid_idx 
    ON marketo.activities (activityDate, activityTypeId);
