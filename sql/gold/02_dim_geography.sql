-- District -> sub-region -> region. Key -1 holds submissions whose district could not be resolved.

DROP TABLE IF EXISTS gold.dim_geography CASCADE;

CREATE TABLE gold.dim_geography AS
SELECT
    row_number() OVER (ORDER BY region_name, subregion_name, district_name)::int AS geography_key,
    district_name,
    district_code,
    subregion_name,
    region_name,
    region_code
FROM meta.ref_geography
UNION ALL
SELECT -1, 'Unknown', 'UNK', 'Unknown', 'Unknown', 'UNK';

ALTER TABLE gold.dim_geography ADD PRIMARY KEY (geography_key);
CREATE UNIQUE INDEX ux_dim_geography_district ON gold.dim_geography (district_name);
