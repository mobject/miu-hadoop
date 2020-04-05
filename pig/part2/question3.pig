REGISTER piggybank.jar;
movies = LOAD 'movies.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (movieId: int, title: chararray, genres: chararray);
movies = FOREACH movies GENERATE movieId, title, STRSPLITTOBAG(genres,'\\|') as genres;
moviesStartWithA = FILTER movies BY STARTSWITH(title,'A') OR STARTSWITH(title,'a');
moviesStartWithA = FOREACH moviesStartWithA GENERATE Flatten(genres) as genres, $0, $1;
grouped = GROUP moviesStartWithA by genres;
count = FOREACH grouped GENERATE group, COUNT($1) as count;
result = ORDER count BY $0;
dump result;