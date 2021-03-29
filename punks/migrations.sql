CREATE TABLE punk_events(
  event_timestamp       VARCHAR(200) NOT NULL
  ,price_eth            NUMERIC(5,3) NOT NULL
  ,price_usd            INTEGER NOT NULL
  ,external_url         VARCHAR(300) NOT NULL
  ,opensea_url          VARCHAR(300) NOT NULL
  ,event_id             VARCHAR(200) NOT NULL PRIMARY KEY
);

CREATE TABLE punks(
  id                    INTEGER NOT NULL PRIMARY KEY
  ,type                 VARCHAR(300) NOT NULL
  ,external_url         VARCHAR(300) NOT NULL
  ,opensea_url          VARCHAR(300) NOT NULL
);
