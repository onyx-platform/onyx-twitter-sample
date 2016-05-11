CREATE TABLE EmojiRank (CountryCode varchar(32) primary key,
                        timespan varchar(32),
                        TotalTweets BIGINT,
                        AverageEmojis BIGINT)
--;;

CREATE TABLE Trending (hashtag varchar(32) primary key,
                       score BIGINT,
                       timespan varchar(32))
