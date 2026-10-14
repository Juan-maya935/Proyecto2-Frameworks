-- =====================================================================
-- DREMIO: consultar la capa Gold (fuente Nessie)
-- Fuente: + Add Source > Nessie
--   Endpoint: http://nessie:19120/api/v2   | Auth: None
--   Storage: AWS access key admin / secret password, root path vacio
--   Connection properties:
--     fs.s3a.endpoint = minio:9000
--     fs.s3a.path.style.access = true
--     dremio.s3.compat = true
--   Desmarcar "Encrypt connection"
-- =====================================================================

-- Top usuarios por actividad
SELECT user_id, display_name, reputation, total_posts, total_questions,
       total_answers, avg_score, nivel_actividad, respuestas_por_pregunta, fecha_cargue
FROM nessie.gold.cant_post_x_user_hist
ORDER BY total_posts DESC
LIMIT 50;

-- Distribucion de usuarios por nivel de actividad (regla de negocio)
SELECT nivel_actividad, COUNT(*) AS usuarios, ROUND(AVG(reputation), 2) AS reputacion_promedio
FROM nessie.gold.cant_post_x_user_hist
GROUP BY nivel_actividad
ORDER BY usuarios DESC;
