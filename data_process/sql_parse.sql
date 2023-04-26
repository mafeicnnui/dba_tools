SELECT
 a.db_desc,b.schema,b.table
FROM    t_db_source a,t_user_query_grants b
WHERE a.id=b.dbid