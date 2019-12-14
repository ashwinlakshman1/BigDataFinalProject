-- 1. Load data from the dataset
RAW_DATA = LOAD 'cleaned' USING PigStorage(',') AS (state: chararray, city: chararray, year: int, solved: chararray, vicsex: chararray, vicage: int, vicrace: chararray, perpsex: chararray, perpage: int, perprace: chararray);
PERPRACE_DATA = FOREACH RAW_DATA GENERATE year AS y, perprace AS p;
GROUP_PERPRACE = GROUP PERPRACE_DATA BY (y,p);
COUNT_PERPRACE = FOREACH GROUP_PERPRACE GENERATE FLATTEN(group), (COUNT(PERPRACE_DATA)) AS freq;
STORE COUNT_PERPRACE INTO 'PerpRaceCount' USING PigStorage(',');
