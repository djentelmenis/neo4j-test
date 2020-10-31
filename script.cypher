// Start neo4j
// Create a local database
// Add APOC plugin
// Open database and run

// Created graph database from StackOverflow questions with the tag "neo4j"
WITH "https://api.stackexchange.com/2.2/questions?pagesize=100&order=desc&sort=creation&tagged=neo4j&site=stackoverflow&filter=!5-i6Zw8Y)4W7vpy91PMYsKM-k9yzEsSC1_Uxlf" AS url
CALL apoc.load.json(url,'$.items[?(@.answer_count>0)].answers[*]') YIELD value
MERGE (a:Answer {id: value.answer_id})
  ON CREATE SET a.accepted = value.is_accepted,
                a.shareLink = value.share_link,
                a.lastActivityDate = value.last_activity_date,
                a.creationDate = value.creation_date,
                a.question = value.title,
                a.answer = value.body_markdown,
                a.upVotes = value.up_vote_count,
                a.downVotes = value.down_vote_count,
                a.comments = value.comment_count,
                a.score = value.score
MERGE (q:Question {id: value.question_id})
  ON CREATE SET q.question = value.title,
                q.views = value.view_count
MERGE (a)-[rel:POSTED_TO]->(q)
WITH a as answer, value.owner as value
MERGE (u:User {userId: value.user_id})
  ON CREATE SET u.displayName = value.display_name,
                u.userType = value.user_type,
                u.reputation = value.reputation,
                u.userLink = value.link
MERGE (u)-[rel2:SUBMITTED]->(answer)
RETURN count(answer);

// Convert epoch-time to date
MATCH (a:Answer)
SET a.creationDate = apoc.date.format(a.creationDate, 's', 'yyyy.MM.dd');

MATCH (a:Answer)
SET a.lastActivityDate = apoc.date.format(a.lastActivityDate, 's', 'yyyy.MM.dd');

// Query with 3 filtering conditions
// Get answers to questions that either the accepted answer or have a positive score and don't have any down votes
MATCH (u:User)-[:SUBMITTED]->(a:Answer)-[:POSTED_TO]->(q:Question)
WHERE a.downVotes < 0 AND a.accepted=true OR a.score > 0
RETURN q.question as Question, u.displayName as User, a.accepted as Accepted, a.score as Score, a.downVotes as Downvotes, a.upVotes as Upvotes

// Query with average aggregation and nested sorting
// Get user average score on submitted answers
MATCH (u:User)-[:SUBMITTED]->(a:Answer)
RETURN u.displayName as Name, avg(a.score) as AverageScore
ORDER BY avg(a.score) DESC, u.displayName

// Quey with a filtering condition, max aggregation and sorting
// Get users with their latest accepted answers
MATCH (u:User)-[:SUBMITTED]->(a:Answer)
WHERE a.accepted=true
RETURN u.displayName as Name, max(a.creationDate) as Date
ORDER BY u.displayName

// Query with a TOP n condition, collect aggregation and nested sorting
// Get users with the number of their answers and the top 3 of their newest answers with their acceptance status
MATCH (u:User)-[:SUBMITTED]->(a:Answer)
WITH u, a order by a.creationDate desc
WITH u.displayName as Name, collect(a.creationDate + ' - ' + a.accepted) as Dates
RETURN Name, size(Dates) as Answers, Dates[0..3] as Dates
ORDER BY Name

// Query with a TOP n condition, a filtering condition, collect aggregation and nested sorting
// Get questions that have more than one answer with the users providing the top 5 answers by their score
MATCH (u:User)-[:SUBMITTED]->(a:Answer)-[:POSTED_TO]->(q:Question)
WITH u, a, q order by a.score desc
WITH q.question as Question, collect(u.displayName + ' - ' + a.score) as Users
WHERE size(Users) > 1
RETURN Question, Users[0..3] as Users
ORDER BY Question
