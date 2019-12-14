-- 1. Load data from the dataset
RAW_DATA = LOAD 'cleaned' USING PigStorage(',') AS (state: chararray, city: chararray, year: int, solved: chararray, vicsex: chararray, vicage: int, vicrace: chararray, perpsex: chararray, perpage: int, perprace: chararray);
A = FOREACH RAW_DATA GENERATE perprace as p, vicrace as v;
B = GROUP A by (p,v);
COUNTA = FOREACH B GENERATE group, COUNT(A);
STORE COUNTA INTO 'PerpVicRaceCount' USING PigStorage(',');
