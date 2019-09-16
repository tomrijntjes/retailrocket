DROP TABLE IF EXISTS category_tree;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS item_properties;


CREATE TABLE category_tree
(
  categoryid int,
  parentid int
);

CREATE TABLE events
(
  timestamp bigint,
  visitorid int,
  event varchar(255),
  itemid int,
  transactionid int
);

CREATE TABLE item_properties
(
  timestamp bigint,
  itemid int,
  property varchar(255),
  value text
);


COPY category_tree FROM '/retailrocketdata/category_tree.csv' DELIMITER ',' CSV HEADER;
COPY events FROM '/retailrocketdata/events.csv' DELIMITER ',' CSV HEADER;
COPY item_properties FROM '/retailrocketdata/item_properties_part1.csv' DELIMITER ',' CSV HEADER;
COPY item_properties FROM '/retailrocketdata/item_properties_part2.csv' DELIMITER ',' CSV HEADER;
