REGISTER piggybank.jar;
movies = LOAD 'movies.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (movieId: int, title: chararray, genres: chararray);
ratings = LOAD 'rating.txt' USING PigStorage() AS (userId: int, movieId: int, rating: int);

movies = FOREACH movies GENERATE movieId, title, STRSPLITTOBAG(genres,'\\|') as genres;
movies = FOREACH movies GENERATE Flatten(genres) as genres, $0, $1;

joins = JOIN movies BY movieId, ratings BY movieId;
adventureMovies = FILTER joins BY genres == 'Adventure' AND rating == 5;
adventureMovies = FOREACH adventureMovies GENERATE movies::movieId as MovieId, genres as Genre, title as Title, rating as Rating;
adventureMovies = DISTINCT adventureMovies;
orderedMovies = ORDER adventureMovies BY MovieId;
top20 = LIMIT orderedMovies 20;
STORE top20 INTO 'output-question6' USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');