CREATE TABLE sb_assets(
  id                   INTEGER NOT NULL PRIMARY KEY,
  token_id             VARCHAR(100) NOT NULL,
  x                    INTEGER NOT NULL,
  y                    INTEGER NOT NULL,
  external_url         VARCHAR(300) NOT NULL,
  opensea_url          VARCHAR(300) NOT NULL
);

CREATE TABLE sb_assets2 (
  id                   INTEGER PRIMARY KEY AUTO_INCREMENT,
  asset_id             INTEGER NOT NULL,
  token_id             VARCHAR(100) NOT NULL,
  x                    INTEGER NOT NULL,
  y                    INTEGER NOT NULL,
  external_url         VARCHAR(300) NOT NULL,
  opensea_url          VARCHAR(300) NOT NULL
);

CREATE TABLE sb_events(
  id                    VARCHAR(200) NOT NULL PRIMARY KEY,
  asset_id              INTEGER NOT NULL,
  asset_token_id        VARCHAR(200) NOT NULL,
  event_type            VARCHAR(200) NOT NULL,
  event_timestamp       VARCHAR(200) NOT NULL,
  amount_eth            NUMERIC(10,3),
  amount_usd            INTEGER,
  seller_address        VARCHAR(200),
  buyer_address         VARCHAR(200),
  opensea_url           VARCHAR(300) NOT NULL,
  updated_timestamp     VARCHAR(200) NOT NULL
);
