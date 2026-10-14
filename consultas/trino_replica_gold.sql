-- =====================================================================
-- TRINO (DBeaver): replicar la tabla Gold a partir de las tablas Silver
-- Conexion: host localhost, puerto 8080, usuario admin, catalogo iceberg
-- =====================================================================

-- 0) Verificar que Trino ve las tablas de Nessie
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.silver;
SHOW TABLES FROM iceberg.gold;

-- 1) Replica de nessie.gold.cant_post_x_user_hist usando SOLO Silver
WITH posts_por_usuario AS (
    SELECT
        owner_user_id,
        COUNT(*)                                                    AS total_posts,
        SUM(CASE WHEN post_type_id = 'Question' THEN 1 ELSE 0 END)  AS total_questions,
        SUM(CASE WHEN post_type_id = 'Answer'   THEN 1 ELSE 0 END)  AS total_answers,
        SUM(score)                                                  AS total_score,
        ROUND(AVG(score), 2)                                        AS avg_score,
        SUM(view_count)                                             AS total_views,
        SUM(comment_count)                                          AS total_comments,
        MIN(creation_date)                                          AS first_post_date,
        MAX(creation_date)                                          AS last_post_date
    FROM iceberg.silver.post_hist
    WHERE owner_user_id IS NOT NULL
    GROUP BY owner_user_id
)
SELECT
    u.id                              AS user_id,
    u.display_name,
    u.reputation,
    u.location,
    u.creation_date                   AS user_creation_date,
    COALESCE(p.total_posts, 0)        AS total_posts,
    COALESCE(p.total_questions, 0)    AS total_questions,
    COALESCE(p.total_answers, 0)      AS total_answers,
    COALESCE(p.total_score, 0)        AS total_score,
    COALESCE(p.avg_score, 0)          AS avg_score,
    COALESCE(p.total_views, 0)        AS total_views,
    COALESCE(p.total_comments, 0)     AS total_comments,
    p.first_post_date,
    p.last_post_date,
    CASE
        WHEN COALESCE(p.total_posts, 0) = 0 THEN 'Sin actividad'
        WHEN p.total_posts < 5  THEN 'Bajo'
        WHEN p.total_posts < 20 THEN 'Medio'
        ELSE 'Alto'
    END                               AS nivel_actividad,
    CASE WHEN p.total_questions > 0
         THEN ROUND(CAST(p.total_answers AS DOUBLE) / p.total_questions, 2)
    END                               AS respuestas_por_pregunta
FROM iceberg.silver.users_hist u
LEFT JOIN posts_por_usuario p ON u.id = p.owner_user_id
ORDER BY total_posts DESC
LIMIT 50;

-- 2) Comparar contra la tabla Gold real (deben coincidir los top usuarios)
SELECT user_id, display_name, total_posts, total_questions, total_answers, nivel_actividad
FROM iceberg.gold.cant_post_x_user_hist
ORDER BY total_posts DESC
LIMIT 50;
