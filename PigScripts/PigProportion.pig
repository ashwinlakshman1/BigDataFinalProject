-- 1. Load data from the dataset
RAW_DATA = LOAD 'cleaned' USING PigStorage(',') AS (state: chararray, city: chararray, year: int, solved: chararray, vicsex: chararray, vicage: int, vicrace: chararray, perpsex: chararray, perpage: int, perprace: chararray);

A = FOREACH RAW_DATA GENERATE year AS y, state AS st, solved AS s;
B = GROUP A BY (y,st);
COUNT_TOTAL = FOREACH B { C = FILTER A BY (s=='Yes');
GENERATE group, COUNT(A) AS tot, COUNT(C) AS del, (float) COUNT(C)/COUNT(A) AS frac;}
dump COUNT_TOTAL;
STORE COUNT_TOTAL INTO 'SolvedProportion' USING PigStorage(',');