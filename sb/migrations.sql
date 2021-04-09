CREATE TABLE sb_assets(
  id                   INTEGER NOT NULL PRIMARY KEY,
  token_id             VARCHAR(100) NOT NULL,
  x                    INTEGER NOT NULL,
  y                    INTEGER NOT NULL,
  external_url         VARCHAR(300) NOT NULL,
  opensea_url          VARCHAR(300) NOT NULL
);
