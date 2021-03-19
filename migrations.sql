DROP TABLE sales;

CREATE TABLE sales(
  sale_timestamp        VARCHAR(200) NOT NULL
  ,size                 INTEGER  NOT NULL
  ,price_usd            INTEGER  NOT NULL
  ,price_usd_parcel     INTEGER  NOT NULL
  ,price_eth            NUMERIC(5,3) NOT NULL
  ,price_eth_parcel     NUMERIC(5,3) NOT NULL
  ,price_mana           INTEGER  NOT NULL
  ,price_mana_parcel    INTEGER  NOT NULL
  ,land_type            VARCHAR(200) NOT NULL
  ,distance_to_road     INTEGER
  ,distance_to_district INTEGER
  ,distance_to_plaza    INTEGER
  ,dcl_url              VARCHAR(300) NOT NULL
  ,opensea_url          VARCHAR(300) NOT NULL
  ,seller_address       VARCHAR(300) NOT NULL
  ,buyer_address        VARCHAR(300) NOT NULL
  ,tx_id                VARCHAR(300) NOT NULL PRIMARY KEY,
  updated_timestamp     VARCHAR(200) NOT NULL
);
