/* ============================================
 * part2.sql
 * ============================================
 * Проект: Info21
 * Участники команды:
 * - ullertyr (Team Leader) - Москва
 * - hbombur - Москва
 * - molliekh - Новосибирск
 * ============================================
 * Описание:
 * Этот файл содержит процедуры и триггеры
 * для изменения данных в базе данных.
 * ============================================
 * Структура:
 * 1. Процедуры добавления P2P-проверки
 * 2. Процедуры добавления проверки Verter'ом
 * 3. Триггер для обновления TransferredPoints
 * 4. Триггер для проверки корректности данных в таблице XP
 * ============================================
 * Примечание:
 * Включает тестовые вызовы для всех процедур и триггеров.
 * ============================================
 */

-- 1. Процедуры добавления P2P-проверки
CREATE OR REPLACE PROCEDURE add_p2p_check(
    checked_peer VARCHAR,
    checking_peer VARCHAR,
    task_title VARCHAR,
    p2p_status status,
    check_time TIME
)
AS
    $$
    DECLARE
        check_id BIGINT;
        new_pk BIGINT;
    BEGIN
        SELECT COALESCE(MAX(pk_p2p), 0) + 1 INTO new_pk FROM p2p;
        IF p2p_status = 'Start' THEN
            INSERT INTO checks (name_peers, title_tasks, date)
            VALUES (checked_peer, task_title, CURRENT_DATE)
            RETURNING pk_checks INTO check_id;
        ELSE
            SELECT c.pk_checks INTO check_id
            FROM checks c
            JOIN p2p p on c.pk_checks = p.id_checks
            WHERE c.name_peers = checked_peer
            AND c.title_tasks = task_title
            AND p.name_peers = checking_peer
            AND state = 'Start'
            AND NOT EXISTS(SELECT 1
                    FROM p2p
                    WHERE id_checks = c.pk_checks
                    AND state in ('Success', 'Failure'))
            ORDER BY p.time DESC
            LIMIT 1;
        END IF;

        IF check_id IS NULL THEN
            RAISE EXCEPTION 'No active P2P check found';
        END IF;

        INSERT INTO p2p (pk_p2p, id_checks, name_peers, state, time)
        VALUES (new_pk, check_id, checking_peer, p2p_status, check_time);
    END;
    $$ LANGUAGE plpgsql;



-- 2. Процедуры добавления проверки Verter'ом
CREATE OR REPLACE PROCEDURE add_verter_check(
    checked_peer VARCHAR,
    task_title VARCHAR,
    state status,
    check_time TIME
)
AS
    $$
    DECLARE
        check_id BIGINT;
        new_pk BIGINT;
    BEGIN
        SELECT COALESCE(MAX(pk_verter), 0) + 1 INTO new_pk FROM verter;
        SELECT c.pk_checks INTO check_id
        FROM checks c
        JOIN p2p p on c.pk_checks = p.id_checks
        WHERE c.name_peers = checked_peer
        AND c.title_tasks = task_title
        AND p.state = 'Success'
        ORDER BY p.time DESC
        LIMIT 1;

        IF check_id IS NULL THEN
            RAISE EXCEPTION 'No successful P2P check found for % in %', checked_peer, task_title;
        END IF;

        INSERT INTO verter (pk_verter, id_cheсks, state, time)
        VALUES (new_pk, check_id, state, check_time);
    END;
    $$ LANGUAGE plpgsql;

-- 3. Триггер для обновления TransferredPoints
CREATE OR REPLACE FUNCTION fnc_update_transferred_points()
RETURNS TRIGGER AS
    $$
    DECLARE
        checked_peer VARCHAR;
    BEGIN
        SELECT name_peers INTO checked_peer
        FROM checks
        WHERE pk_checks = NEW.id_checks;

        INSERT INTO transferred_points(checking_peers, checked_peers, points)
        VALUES (NEW.name_peers, checked_peer, 1);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;


CREATE TRIGGER update_transferred_points
    AFTER INSERT ON p2p
    FOR EACH ROW
    WHEN (NEW.state = 'Start')
    EXECUTE FUNCTION fnc_update_transferred_points();

-- 4. Триггеры xp для проверки состояния задачи
CREATE OR REPLACE FUNCTION fnc_check_amount_xp()
    RETURNS TRIGGER AS
    $$
        BEGIN
            IF NOT EXISTS (select 1
                    FROM p2p
                    WHERE id_checks = NEW.id_checks
                    AND state = 'Success'
                ) THEN
                    RAISE EXCEPTION 'Check is not successful';
            END IF;

            IF EXISTS(select 1
                    FROM verter
                    WHERE id_cheсks = NEW.id_checks
                ) AND NOT EXISTS(select 1
                    FROM verter
                    WHERE id_cheсks = NEW.id_checks
                    AND state = 'Success'
                ) THEN
                    RAISE EXCEPTION 'Verter check is not successful';
            END IF;

            IF NEW.xp_amount > (select max_xp
                    FROM tasks t
                    JOIN checks c on t.pk_title = c.title_tasks
                    WHERE c.pk_checks = NEW.id_checks
                ) THEN
                    RAISE EXCEPTION 'XP amount exceeds the maximum';
            END IF;
            RETURN NEW;
        END;
    $$ LANGUAGE plpgsql;

CREATE OR REPLACE  TRIGGER trg_check_amount_xp
    BEFORE INSERT OR UPDATE
    ON xp
    FOR EACH ROW
    EXECUTE FUNCTION fnc_check_amount_xp();

-- 5. Тригер на успешную проверку P2P и Verter
CREATE OR REPLACE FUNCTION fnc_handle_xp()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME = 'p2p' THEN
        INSERT INTO xp (id_checks, xp_amount)
        SELECT c.pk_checks, t.max_xp
        FROM checks c
        JOIN tasks t ON c.title_tasks = t.pk_title
        WHERE c.pk_checks = NEW.id_checks;

    ELSIF TG_TABLE_NAME = 'verter' AND NEW.state = 'Failure' THEN
            DELETE
            FROM xp
            WHERE id_checks = NEW.id_cheсks;
        END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER trg_p2p_xp
AFTER INSERT ON p2p
FOR EACH ROW
WHEN (NEW.state = 'Success')
EXECUTE FUNCTION fnc_handle_xp();

CREATE OR REPLACE TRIGGER trg_verter_xp
AFTER INSERT ON verter
FOR EACH ROW
WHEN (NEW.state = 'Failure' OR NEW.state = 'Success')
EXECUTE FUNCTION fnc_handle_xp();

-- call fnc_handle_xp();


-- Тестовые вызовы

-- Блок 1: Инициализация данных
TRUNCATE TABLE peers, tasks, checks, p2p, verter, transferred_points,
    friends, xp, time_tracking RESTART IDENTITY CASCADE;
INSERT INTO peers (pk_peers, birthday) VALUES
('peer1', '2000-01-01'),
('peer2', '2000-02-02'),
('peer3', '2000-03-03'),
('peer4', '2000-04-04');
INSERT INTO tasks (pk_title, par_task, max_xp) VALUES
('C3', NULL, 100),
('C4', 'C3', 200),
('C5', 'C4', 300);

-- Блок 2: Базовые P2P проверки
CALL add_p2p_check('peer1', 'peer2', 'C3', 'Start', '15:30');
CALL add_p2p_check('peer1', 'peer2', 'C3', 'Success', '15:35');
CALL add_p2p_check('peer3', 'peer4', 'C5', 'Start', '15:40');
CALL add_p2p_check('peer3', 'peer4', 'C5', 'Failure', '15:42');
CALL add_p2p_check('peer2', 'peer4', 'C4', 'Start', '15:43');
CALL add_p2p_check('peer2', 'peer4', 'C4', 'Success', '15:45');
SELECT * FROM transferred_points;
SELECT * FROM xp;

-- Блок 3: Verter проверки
CALL add_verter_check('peer1', 'C3', 'Start', '15:45');
CALL add_verter_check('peer1', 'C3', 'Success', '15:50');
CALL add_verter_check('peer2', 'C4', 'Start', '15:55');
CALL add_verter_check('peer2', 'C4', 'Failure', '15:57');
SELECT * FROM verter;
SELECT * FROM xp;

-- Блок 4: Проверка взаимности друзей
INSERT INTO friends (pk_friends, first_peers, second_peers) VALUES (100, 'peer1', 'peer3');
SELECT * FROM friends WHERE first_peers = 'peer3';
SELECT * FROM friends WHERE second_peers = 'peer1';

-- Блок 5: XP и TimeTracking
CALL add_p2p_check('peer2', 'peer4', 'C4', 'Start', '10:00');
CALL add_p2p_check('peer2', 'peer4', 'C4', 'Success', '10:30');
INSERT INTO xp (id_checks, xp_amount) VALUES (currval('checks_pk_checks_seq'), 200);
INSERT INTO time_tracking (pk_time_tracking, name_peers, date, time, state) VALUES
(1, 'peer4', CURRENT_DATE, '09:00', 1);
SELECT * FROM xp;
SELECT * FROM time_tracking;

-- Блок 6: Ошибочные сценарии
CALL add_p2p_check('ghost_peer', 'peer1', 'C3', 'Start', '11:00');
CALL add_p2p_check('peer3', 'peer4', 'UnknownTask', 'Start', '12:00');
CALL add_p2p_check('peer3', 'peer4', 'C5', 'Start', '14:00');
CALL add_p2p_check('peer4', 'peer3', 'C5', 'Success', '15:45');
SELECT * FROM checks WHERE name_peers = 'ghost_peer';
SELECT * FROM p2p WHERE name_peers = 'peer4';

-- Блок 7: Итоговые проверки
SELECT 'TransferredPoints' AS table_name, * FROM transferred_points;
SELECT 'P2P' AS table_name, * FROM p2p;
SELECT 'Checks' AS table_name, * FROM checks;
SELECT 'Verter' AS table_name, * FROM verter;
SELECT 'XP' AS table_name, * FROM xp;


