INSTALL httpfs;
INSTALL iceberg;
LOAD httpfs;
LOAD iceberg;
CREATE OR REPLACE SECRET rustfs (
    TYPE s3,
    KEY_ID 'picomq',
    SECRET 'picomqpicomq',
    REGION 'us-east-1',
    ENDPOINT 'rustfs:9000',
    URL_STYLE 'path',
    USE_SSL false
);
ATTACH 's3://lake/warehouse/' AS lake (
    TYPE iceberg,
    ENDPOINT 'http://iceberg-rest:8181',
    AUTHORIZATION_TYPE 'none'
);
