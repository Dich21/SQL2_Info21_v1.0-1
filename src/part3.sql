/* ============================================
 * part3.sql
 * ============================================
 * Проект: Info21
 * Участники команды:
 * - ullertyr (Team Leader) - Москва
 * - hbombur - Москва
 * - molliekh - Новосибирск
 * ============================================
 * Описание:
 * Этот файл содержит реализации SQL-функций и процедур
 * для выполнения аналитических запросов к данным «Школы 21».
 * Включает решения 17 задач на получение данных из БД.
 * ============================================
 * Структура:
 * 1. Функции преобразования данных (задачи 1-3)
 * 2. Процедуры анализа пир-поинтов (задачи 4-5)
 * 3. Статистика по проверкам (задачи 6-7, 12-14)
 * 4. Социальные взаимодействия (задачи 8, 11)
 * 5. Анализ посещаемости (задачи 3, 15-17)
 * 6. Блочная аналитика (задачи 7, 9)
 * 7. Специфические проверки (задачи 10, 13)
 * ============================================
 */

-- 1. Функция для преобразования TransferredPoints
CREATE OR REPLACE FUNCTION fnc_transferred_points()
RETURNS TABLE(Peer1 VARCHAR, Peer2 VARCHAR, PointsAmount INT) AS $$
BEGIN
    RETURN QUERY
    WITH tp1 AS (
        SELECT checking_peers AS p1, checked_peers AS p2, SUM(points) AS points_sent
        FROM transferred_points
        GROUP BY checking_peers, checked_peers
    ),
    tp2 AS (
        SELECT checked_peers AS p1, checking_peers AS p2, SUM(points) AS points_received
        FROM transferred_points
        GROUP BY checked_peers, checking_peers
    )
    SELECT COALESCE(tp1.p1, tp2.p1) AS Peer1, COALESCE(tp1.p2, tp2.p2) AS Peer2,
           (COALESCE(tp1.points_sent, 0) - COALESCE(tp2.points_received, 0))::INT AS PointsAmount
    FROM tp1
    FULL JOIN tp2 ON tp1.p1 = tp2.p1 AND tp1.p2 = tp2.p2
    ORDER BY Peer1, Peer2;
END;
$$ LANGUAGE plpgsql;

select *
from fnc_transferred_points()
ORDER BY peer1, peer2;

-- 2. Функция для успешных проверок с XP
CREATE OR REPLACE FUNCTION fnc_successful_checks()
    RETURNS TABLE(Peer VARCHAR, Task VARCHAR, XP INT)
    AS $$
        BEGIN
            RETURN QUERY
            SELECT c.name_peers, c.title_tasks, xp_amount
            FROM checks c
            JOIN xp ON c.pk_checks = xp.id_checks
            JOIN tasks on c.title_tasks = tasks.pk_title
            WHERE xp_amount <= tasks.max_xp
            AND EXISTS(SELECT 1
                       FROM p2p
                       WHERE id_checks = c.pk_checks
                       AND state = 'Success')
            AND (NOT EXISTS(SELECT 1
                            FROM verter
                            WHERE verter.id_cheсks = c.pk_checks
                            AND state = 'Failure')
                OR NOT EXISTS(SELECT 1
                            FROM verter
                            WHERE verter.id_cheсks = c.pk_checks));
        END;
    $$ LANGUAGE plpgsql;

SELECT * FROM fnc_successful_checks();

-- 3. Функция для пиров, не выходивших весь день
CREATE OR REPLACE FUNCTION fnc_all_day_in_campus(check_day DATE)
RETURNS TABLE (Peer VARCHAR)
    AS $$
        BEGIN
            RETURN QUERY
            SELECT name_peers
            FROM time_tracking
            WHERE date = check_day
            GROUP BY name_peers
            HAVING COUNT(*) FILTER (WHERE state > 1) > COUNT (*) FILTER (WHERE state = 2)
            OR MAX(state) = 1;
        END;
    $$ LANGUAGE plpgsql;

SELECT * FROM fnc_all_day_in_campus('2022-05-25');

-- 4. Изменение пир-поинтов по TransferredPoints
CREATE OR REPLACE FUNCTION fnc_points_change_transferred_points()
RETURNS TABLE (Peer VARCHAR, PointsChange INT) AS $$
BEGIN
    RETURN QUERY
    SELECT p.pk_peers AS Peer,
        (COALESCE(t_checking.total_given, 0) - COALESCE(t_checked.total_received, 0))::INT AS PointsChange
    FROM peers p
    LEFT JOIN (
    SELECT checking_peers, COUNT(*) AS count_checking, SUM(points) AS total_given
    FROM transferred_points
    GROUP BY checking_peers
    ) t_checking ON p.pk_peers = t_checking.checking_peers
    JOIN (
        SELECT checked_peers, COUNT(*) AS count_checked, SUM(points) AS total_received
        FROM transferred_points
        GROUP BY checked_peers
    ) t_checked ON p.pk_peers = t_checked.checked_peers
    ORDER BY PointsChange;
END;
$$ LANGUAGE plpgsql;

select *
from fnc_points_change_transferred_points();

-- 5. Изменение пир-поинтов через функцию из п.1
CREATE OR REPLACE FUNCTION prc_points_change_human_readable()
RETURNS TABLE (Peer   VARCHAR, Points INT)
AS
$$
SELECT peer1 AS peer, SUM(pointsamount) AS change_points
FROM (SELECT peer1, SUM(pointsamount) AS pointsamount
      FROM fnc_transferred_points()
      GROUP BY peer1
      ) AS table_points
GROUP BY peer1
ORDER BY change_points;
$$ LANGUAGE SQL;

SELECT  *
    FROM prc_points_change_human_readable();

-- 6. Часто проверяемые задания по дням
CREATE OR REPLACE FUNCTION fnc_most_checked_tasks_per_day()
RETURNS TABLE (Day DATE, Task VARCHAR)
AS $$
    BEGIN
        RETURN QUERY
        WITH task_counts as (
            SELECT date, title_tasks, COUNT(*) as checks_count
            FROM checks
            GROUP BY date, title_tasks
        ),
            max_counts AS (
                SELECT date, MAX(checks_count) as max_count
                FROM task_counts
                GROUP BY date
                )
        SELECT tc.date AS Dat,
                tc.title_tasks AS Task
        FROM task_counts tc
        JOIN max_counts mc
        ON tc.date = mc.date
        WHERE tc.checks_count = mc.max_count
        ORDER BY tc.date DESC;
    END;
$$ LANGUAGE plpgsql;

SELECT  *
    FROM fnc_most_checked_tasks_per_day();

-- 7. Пир, завершивший блок задач
CREATE OR REPLACE PROCEDURE prc_completed_block
    (IN block_name VARCHAR, INOUT ref REFCURSOR = 'ref')
AS $$
BEGIN
    OPEN ref FOR
    WITH block_tasks AS (
        SELECT pk_title,
            CAST(SUBSTRING(pk_title FROM '\d+') AS INTEGER) AS task_num
        FROM Tasks
        WHERE pk_title LIKE block_name || '%'
    ),
    max_task AS (
        SELECT MAX(task_num) AS max_num
        FROM block_tasks
    ),
    completed_tasks AS (
        SELECT c.name_peers, CAST(SUBSTRING(title_tasks FROM '\d+') AS INTEGER) AS task_num,
               c.date AS task_date
        FROM checks c
        JOIN p2p ON c.pk_checks = p2p.id_checks AND p2p.state = 'Success'
        LEFT JOIN verter v ON c.pk_checks = v.id_cheсks
        AND (v.state = 'Success' OR v.pk_verter IS NULL)
        WHERE title_tasks LIKE block_name || '%'
        AND CAST(SUBSTRING(title_tasks FROM '\d+') AS INTEGER) <= (SELECT max_num FROM max_task)
    )
    SELECT name_peers, MAX(task_date) AS last_task
    FROM completed_tasks
    GROUP BY name_peers
    HAVING COUNT(DISTINCT task_num) = (SELECT max_num FROM max_task)
    ORDER BY last_task DESC;
end;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_completed_block('SQL', 'ref');
FETCH ALL IN "ref";
COMMIT;


-- 8. Рекомендуемые проверяющие
CREATE OR REPLACE PROCEDURE prc_recommended_peer(
    OUT peer_name TEXT,
    OUT recommended_peer_name TEXT
) AS $$
BEGIN
    WITH friend_recommendations AS (
        SELECT f.first_peers AS peer, r.to_peers AS recommended_peer
        FROM friends f
        JOIN recommendations r ON f.second_peers = r.from_peers
    )
    SELECT peer, recommended_peer
    INTO peer_name, recommended_peer_name
    FROM (
        SELECT peer, recommended_peer,
            RANK() OVER (PARTITION BY peer ORDER BY COUNT(*) DESC) AS rank
        FROM friend_recommendations
        GROUP BY peer, recommended_peer
    ) AS ranked
    WHERE rank = 1
    LIMIT 1;

END;
$$ LANGUAGE plpgsql;

CALL prc_recommended_peer(null, null);

-- 9. Проценты по блокам
CREATE OR REPLACE FUNCTION fnc_block_percentage(block1 VARCHAR, block2 VARCHAR)
RETURNS TABLE (
    StartedBlock1 numeric,
    StartedBlock2 numeric,
    StartedBothBlocks numeric,
    DidntStartAnyBlock numeric
) AS $$
WITH peer_flags AS (
    SELECT
        p.pk_peers,
        EXISTS(SELECT 1 FROM checks
               WHERE name_peers = p.pk_peers
               AND title_tasks LIKE block1 || '%') AS started1,
        EXISTS(SELECT 1 FROM checks
               WHERE name_peers = p.pk_peers
               AND title_tasks LIKE block2 || '%') AS started2
    FROM peers p
),
categories AS (
    SELECT
        COUNT(*) FILTER (WHERE started1 AND NOT started2) AS only1,
        COUNT(*) FILTER (WHERE started2 AND NOT started1) AS only2,
        COUNT(*) FILTER (WHERE started1 AND started2) AS both_blocks,
        COUNT(*) FILTER (WHERE NOT started1 AND NOT started2) AS none
    FROM peer_flags
)
SELECT
    ROUND(only1 * 100.0 / NULLIF(only1 + only2 + both_blocks + none, 0), 0) AS StartedBlock1,
    ROUND(only2 * 100.0 / NULLIF(only1 + only2 + both_blocks + none, 0), 0) AS StartedBlock2,
    ROUND(both_blocks * 100.0 / NULLIF(only1 + only2 + both_blocks + none, 0), 0) AS StartedBothBlocks,
    ROUND(none * 100.0 / NULLIF(only1 + only2 + both_blocks + none, 0), 0) AS DidntStartAnyBlock
FROM categories;
$$ LANGUAGE SQL;

SELECT * FROM fnc_block_percentage('C', 'DO');

-- 10. Проверки в день рождения
CREATE OR REPLACE FUNCTION fnc_birthday_checks()
RETURNS TABLE(SuccessfulChecks numeric, UnsuccessfulChecks numeric) AS $$
WITH birthday_checks AS (
    SELECT
        c.pk_checks,
        p.birthday,
        CASE WHEN p2p.state = 'Success' AND
                  (verter.state IS NULL OR verter.state = 'Success') THEN 1 ELSE 0 END AS success,
        CASE WHEN p2p.state = 'Failure' OR verter.state = 'Failure' THEN 1 ELSE 0 END AS fail
    FROM checks c
    JOIN peers p ON c.name_peers = p.pk_peers
    LEFT JOIN p2p ON c.pk_checks = p2p.id_checks AND p2p.state IN ('Success', 'Failure')
    LEFT JOIN verter ON c.pk_checks = verter.id_cheсks AND verter.state IN ('Success', 'Failure')
    WHERE TO_CHAR(c.date, 'MM-DD') = TO_CHAR(p.birthday, 'MM-DD')
)
SELECT
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN success = 1 THEN pk_checks END) / NULLIF(COUNT(DISTINCT pk_checks), 0), 0),
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN fail = 1 THEN pk_checks END) / NULLIF(COUNT(DISTINCT pk_checks), 0), 0)
FROM birthday_checks;
$$ LANGUAGE SQL;

SELECT * FROM fnc_birthday_checks();


-- 11. Пир сдал 1 и 2, но не 3
CREATE OR REPLACE PROCEDURE prc_completed_tasks(task1 VARCHAR, task2 VARCHAR,
                                                task3 VARCHAR, INOUT ref REFCURSOR = 'ref')
AS $$
BEGIN
    OPEN ref FOR
    WITH successful_checks AS (
        SELECT c.name_peers, c.title_tasks
        FROM checks c
        JOIN p2p ON c.pk_checks = p2p.id_checks AND p2p.state = 'Success'
        JOIN verter v ON c.pk_checks = v.id_cheсks
        WHERE (v.state IS NULL OR v.state = 'Success')
    )
    SELECT DISTINCT s1.name_peers AS Peer
    FROM successful_checks s1
    JOIN successful_checks s2
        ON s1.name_peers = s2.name_peers
        AND s2.title_tasks = task2
    WHERE s1.title_tasks = task1
    EXCEPT
    SELECT name_peers
    FROM successful_checks
    WHERE title_tasks = task3;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_completed_tasks('C1', 'C2', 'DO2', 'ref');
FETCH ALL IN "ref";
COMMIT;

-- 12. Рекурсия проектов
CREATE OR REPLACE PROCEDURE prc_prev_tasks_count(INOUT ref REFCURSOR = 'ref') AS $$
BEGIN
    OPEN ref FOR
    WITH RECURSIVE task_hierarchy AS (
        SELECT
            pk_title AS task,
            par_task AS parent,
            0 AS prev_count
        FROM tasks
        WHERE par_task IS NULL

        UNION ALL

        SELECT
            t.pk_title,
            t.par_task,
            th.prev_count + 1
        FROM tasks t
        JOIN task_hierarchy th ON t.par_task = th.task
    )
    SELECT
        task,
        prev_count AS "PrevCount"
    FROM task_hierarchy
    ORDER BY task;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_prev_tasks_count('ref');
FETCH ALL IN "ref";
COMMIT;

-- 13. Удачные дни для проверки
CREATE OR REPLACE PROCEDURE prc_lucky_days(N INT, INOUT ref REFCURSOR = 'ref') AS $$
BEGIN
    OPEN ref FOR
    WITH checks_data AS (
        SELECT c.date, p2p.time, x.xp_amount, t.max_xp,
            CASE
                WHEN p2p.state = 'Success'
                AND (verter.state IS NULL OR verter.state = 'Success')
                AND x.xp_amount >= 0.8 * t.max_xp THEN 1
                ELSE 0
            END AS is_valid
        FROM checks c
        JOIN p2p ON c.pk_checks = p2p.id_checks
        LEFT JOIN verter ON c.pk_checks = verter.id_cheсks
        JOIN tasks t ON c.title_tasks = t.pk_title
        LEFT JOIN xp x ON c.pk_checks = x.id_checks
        WHERE p2p.state != 'Start'
    ),
    sequences AS (
        SELECT date, SUM(CASE WHEN is_valid = 0 THEN 1 ELSE 0 END)
                OVER (ORDER BY date, time) AS grp
        FROM checks_data
        WHERE is_valid = 1
    )
    SELECT date::text
    FROM (
        SELECT date, COUNT(*) AS consecutive_count, grp
        FROM sequences
        GROUP BY date, grp
    ) AS grouped
    WHERE consecutive_count >= N
    GROUP BY date;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_lucky_days(2, 'ref');
FETCH ALL IN "ref";
COMMIT;

-- 14. Пир с наибольшим количеством опыта

CREATE OR REPLACE FUNCTION fnc_max_xp_peer()
RETURNS TABLE (Peer VARCHAR, XP BIGINT)
AS $$
    BEGIN
        RETURN QUERY
        SELECT c.name_peers as Peer, SUM(xp.xp_amount)::BIGINT as XP
        FROM checks c
        JOIN xp ON c.pk_checks = xp.id_checks
        GROUP BY c.name_peers
        ORDER BY XP DESC
        LIMIT 1;
    END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_max_xp_peer();


-- 15. Пир, приходивший раньше заданного времени
CREATE OR REPLACE PROCEDURE prc_early_peers
    (check_time TIME, min_entries INT,
    INOUT ref REFCURSOR = 'ref')
AS $$
    BEGIN
        OPEN ref FOR
        SELECT name_peers AS Peers
        FROM time_tracking
        WHERE time < check_time
        AND state = 1
        GROUP BY name_peers
        HAVING COUNT(*) >= min_entries;
    END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prc_early_peers('15:40:16'::TIME, 2, 'ref');
FETCH ALL IN "ref";
COMMIT;

-- 16. Пиры, выходившие больше m раз
CREATE OR REPLACE FUNCTION fnc_exit_stats(n integer, m integer)
RETURNS TABLE (peer varchar)
AS $$
DECLARE
	start_date date;
BEGIN
	start_date=(SELECT MAX(date) FROM time_tracking) - (n-1);
RETURN QUERY
	SELECT time_tracking.name_peers FROM time_tracking
	WHERE state=2
	AND time_tracking.date>=start_date
	GROUP BY 1
	HAVING COUNT(state) >= m;
END
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_exit_stats(3, 1);

-- 17 Процент ранних входов по месяцам
CREATE OR REPLACE FUNCTION early_arrivals_on_month()
RETURNS TABLE (months char, early_entries numeric) AS
$$ (WITH all_entries AS (SELECT EXTRACT(MONTH FROM date) AS month, COUNT(*) AS counts
                 FROM time_tracking
                 JOIN peers ON time_tracking.name_peers = peers.pk_peers
                 WHERE time_tracking.state = '1'
                 GROUP BY month),
         early_entries AS (SELECT EXTRACT(MONTH FROM date) as month, count(*) AS counts
                 FROM time_tracking
                 JOIN peers ON peers.pk_peers = time_tracking.name_peers
                 WHERE time_tracking.Time < '12:00'
                 AND time_tracking.state = '1'
                 GROUP BY month)
    SELECT TO_CHAR(TO_DATE(all_entries.month::text, 'MM'), 'Month') AS months,
           ROUND((CAST(sum(early_entries.counts) AS numeric) * 100) / CAST(sum(all_entries.counts) AS numeric), 0) AS early_entries
    FROM all_entries
    JOIN early_entries ON all_entries.month = early_entries.month
    GROUP BY all_entries.month
    ORDER BY all_entries.month
);
$$ LANGUAGE sql;

SELECT * FROM early_arrivals_on_month();
