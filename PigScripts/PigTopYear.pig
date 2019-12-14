-- 1. Load data from teh dataset
RAW_DATA = LOAD 'A_homicide_by_year' USING PigStorage('\t') AS (year: int, deaths: int);
-- 2. Top 10 Years with most homicides
TOPYEAR = FOREACH RAW_DATA GENERATE year AS y, deaths AS d;
-- 3. Grouping by year,
GROUP_TOPYEAR = GROUP TOPYEAR BY (y,d);
-- 4. Aggregate, flatten
COUNT_TOPYEAR = FOREACH GROUP_TOPYEAR GENERATE FLATTEN(group), COUNT(TOPYEAR) as count;
-- 5.5 Aggregate over year
GROUP_COUNT_TOPYEAR = GROUP COUNT_TOPYEAR BY y;
-- 6. UDF to compute top 10
topDeathYear = FOREACH GROUP_COUNT_TOPYEAR { result = TOP(20, 2, COUNT_TOPYEAR);
GENERATE FLATTEN(result);
}
-- dump
STORE topDeathYear INTO 'PigOutput1' USING PigStorage(',');
