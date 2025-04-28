/* ============================================
 * part1.sql
 * ============================================
 * Проект: Info21
 * Участники команды:
 * - ullertyr (Team Leader) - Москва
 * - hbombur - Москва
 * - molliekh - Новосибирск
 * ============================================
 * Описание:
 * Этот файл отвечает за создание базы данных,
 * включая таблицы, процедуры для импорта/экспорта данных
 * и начальное наполнение таблиц.
 * ============================================
 * Структура:
 * 1. Создание таблиц и их структуры
 * 2. Добавление данных в таблицы
 * 3. Процедуры для импорта/экспорта данных (CSV)
 * ============================================
 */

-- Удаление таблиц
-- DROP TABLE IF EXISTS
--     peers,
--     tasks,
--     checks,
--     p2p,
--     verter,
--     transferred_points,
--     friends,
--     recommendations,
--     xp,
--     time_tracking
-- CASCADE;

-- Очистка таблиц
-- TRUNCATE TABLE
--     time_tracking,
--     recommendations,
--     friends,
--     transferred_points,
--     XP,
--     Verter,
--     P2P,
--     Checks,
--     Tasks,
--     Peers
-- RESTART IDENTITY CASCADE;

-- Создание базы данных


DROP TYPE IF EXISTS status;
CREATE TYPE status AS ENUM ('Start', 'Success', 'Failure');

CREATE TABLE IF NOT EXISTS peers (
    pk_peers VARCHAR PRIMARY KEY NOT NULL,
    birthday DATE
);

CREATE TABLE IF NOT EXISTS tasks (
    pk_title VARCHAR PRIMARY KEY NOT NULL,
    par_task VARCHAR,
    max_xp INT NOT NULL,
    CONSTRAINT fk_tasks_parent FOREIGN KEY (par_task) REFERENCES tasks(pk_title)
);

CREATE TABLE IF NOT EXISTS checks(
    pk_checks BIGSERIAL PRIMARY KEY NOT NULL,
    name_peers VARCHAR NOT NULL,
    title_tasks VARCHAR NOT NULL,
    date DATE NOT NULL,
    CONSTRAINT fk_name_peers FOREIGN KEY (name_peers) REFERENCES peers(pk_peers),
    CONSTRAINT fk_title_tasks FOREIGN KEY (title_tasks) REFERENCES tasks(pk_title)
);

-- Функция проверки существования родительской задачи

CREATE OR REPLACE FUNCTION fnc_check_task_exists()
    RETURNS TRIGGER AS
    $$
    BEGIN
        IF NEW.par_task IS NOT NULL AND NOT EXISTS(SELECT 1 FROM tasks WHERE pk_title = NEW.par_task) THEN
            RAISE EXCEPTION 'Parent task does not exist';
        END IF;
        RETURN NEW;
    end;
    $$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_check_task_exists
    BEFORE INSERT OR UPDATE
    ON tasks
    FOR EACH ROW
    EXECUTE FUNCTION fnc_check_task_exists();

CREATE TABLE IF NOT EXISTS p2p (
    pk_p2p BIGINT PRIMARY KEY NOT NULL,
    id_checks BIGINT NOT NULL,
    name_peers VARCHAR NOT NULL,
    state status NOT NULL,
    time TIME NOT NULL,
    CONSTRAINT fk_id_checks FOREIGN KEY (id_checks) REFERENCES checks(pk_checks),
    CONSTRAINT fk_name_peers FOREIGN KEY (name_peers) REFERENCES peers(pk_peers)
);

-- Триггеры p2p для проверки состояния задачи

CREATE OR REPLACE FUNCTION fnc_check_p2p_state()
    RETURNS TRIGGER AS
    $$
    DECLARE
        current_task VARCHAR;
        checked_peer VARCHAR;
    BEGIN
        SELECT title_tasks, name_peers INTO current_task, checked_peer
        FROM checks
        WHERE pk_checks = NEW.id_checks;

        IF NEW.state = 'Start' THEN

            IF EXISTS(SELECT 1
                FROM p2p
                JOIN checks c on c.pk_checks = p2p.id_checks
                WHERE
                c.name_peers = NEW.name_peers -- Проверяющий
                AND c.name_peers = checked_peer -- Проверяемый
                AND c.title_tasks = current_task -- Задание
                AND state = 'Start'
                AND NOT EXISTS(select 1
                                   FROM p2p p2
                                   WHERE p2.id_checks = p2p.id_checks
                                     AND p2.state IN ('Success', 'Failure'))
                ) THEN
                  RAISE EXCEPTION 'P2P check for task "%" already in progress', current_task;
            END IF;

        ELSE

            IF NOT EXISTS(SELECT 1
                FROM p2p
                WHERE id_checks = NEW.id_checks
                AND name_peers = NEW.name_peers
                AND state = 'Start'
                AND time < NEW.time
            ) THEN
                RAISE EXCEPTION 'No active P2P checks at the time';
            END IF;

            IF EXISTS(select 1
                FROM p2p
                WHERE id_checks = NEW.id_checks
                AND state IN ('Success', 'Failure')
            ) THEN
                RAISE EXCEPTION 'P2P check is already completed';
            END IF;

        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_check_p2p_state
    BEFORE INSERT OR UPDATE
    ON p2p
    FOR EACH ROW
    EXECUTE FUNCTION fnc_check_p2p_state();

CREATE TABLE IF NOT EXISTS verter (
    pk_verter BIGINT PRIMARY KEY NOT NULL,
    id_cheсks BIGINT NOT NULL,
    state status NOT NULL,
    time TIME NOT NULL,
    CONSTRAINT fk_id_checks FOREIGN KEY (id_cheсks) REFERENCES checks(pk_checks)
);

-- Триггеры verter для проверки состояния задачи

CREATE OR REPLACE FUNCTION fnc_check_verter_state()
    RETURNS TRIGGER AS
    $$
    BEGIN

        IF NOT EXISTS(select 1
            FROM p2p
            WHERE id_checks = NEW.id_cheсks
            AND state = 'Success'
            ) THEN
            RAISE EXCEPTION 'Peer-to-peer checks are not completed';
        END IF;

        IF NEW.state = 'Start' THEN
            IF EXISTS (
                SELECT 1
                FROM verter
                WHERE id_cheсks = NEW.id_cheсks
                AND state = 'Start'
            ) THEN
                RAISE EXCEPTION 'Verter check already started';
            END IF;
        ELSE
            IF NOT EXISTS (
                SELECT 1
                FROM verter
                WHERE id_cheсks = NEW.id_cheсks
                AND state = 'Start'
            ) THEN
                RAISE EXCEPTION 'Verter check not started';
            END IF;

        IF EXISTS (SELECT 1
            FROM verter
            WHERE id_cheсks = NEW.id_cheсks
            AND state IN ('Success', 'Failure')
            ) THEN
                RAISE EXCEPTION 'Verter check already completed';
            END IF;
        END IF;

        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_check_verter_state
    BEFORE INSERT OR UPDATE
    ON verter
    FOR EACH ROW
    EXECUTE FUNCTION fnc_check_verter_state();

CREATE TABLE IF NOT EXISTS transferred_points (
    pk_transferred_points BIGSERIAL PRIMARY KEY NOT NULL,
    checking_peers VARCHAR NOT NULL,
    checked_peers VARCHAR NOT NULL,
    points INT NOT NULL,
    CONSTRAINT fk_checking_peers FOREIGN KEY (checking_peers) REFERENCES peers(pk_peers),
    CONSTRAINT fk_checked_peers FOREIGN KEY (checked_peers) REFERENCES peers(pk_peers)
);

CREATE TABLE IF NOT EXISTS friends (
    pk_friends BIGINT PRIMARY KEY NOT NULL,
    first_peers VARCHAR NOT NULL,
    second_peers VARCHAR NOT NULL,
    CONSTRAINT fk_first_peers FOREIGN KEY (first_peers) REFERENCES peers(pk_peers),
    CONSTRAINT fk_second_peers FOREIGN KEY (second_peers) REFERENCES peers(pk_peers),
    CONSTRAINT ch_unique_peers CHECK (first_peers <> second_peers)
);

-- Триггеры friends для взаимности дружбы

CREATE OR REPLACE FUNCTION fnc_friends_reciprocity()
    RETURNS TRIGGER AS
    $$
    DECLARE
        next_id BIGINT;
    BEGIN
        IF NOT EXISTS (SELECT 1
        FROM friends
        WHERE first_peers = NEW.second_peers
        AND second_peers = NEW.first_peers
    ) AND NEW.first_peers <> NEW.second_peers THEN
        SELECT COALESCE(MAX(pk_friends), 0) + 1 INTO next_id
            FROM friends;

        INSERT INTO friends (pk_friends, first_peers, second_peers)
        VALUES (next_id, NEW.second_peers, NEW.first_peers);
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_friends_reciprocity
    AFTER INSERT
    ON friends
    FOR EACH ROW
    EXECUTE FUNCTION fnc_friends_reciprocity();

CREATE TABLE IF NOT EXISTS recommendations (
    pk_recommendations BIGSERIAL PRIMARY KEY NOT NULL,
    from_peers VARCHAR NOT NULL,
    to_peers VARCHAR NOT NULL,
    CONSTRAINT fk_from_peers FOREIGN KEY (from_peers) REFERENCES peers(pk_peers),
    CONSTRAINT fk_to_peers FOREIGN KEY (to_peers) REFERENCES peers(pk_peers)
);

CREATE TABLE IF NOT EXISTS xp (
    pk_xp BIGSERIAL PRIMARY KEY NOT NULL,
    id_checks BIGINT NOT NULL,
    xp_amount INT NOT NULL,
    CONSTRAINT fk_id_checks FOREIGN KEY (id_checks) REFERENCES checks(pk_checks)
);

CREATE TABLE IF NOT EXISTS time_tracking (
    pk_time_tracking BIGINT PRIMARY KEY NOT NULL,
    name_peers VARCHAR NOT NULL,
    date DATE NOT NULL,
    time TIME NOT NULL,
    state INT NOT NULL,
    CONSTRAINT fk_name_peers FOREIGN KEY (name_peers) REFERENCES peers(pk_peers),
    CONSTRAINT ch_state CHECK (state in(1,2))
);

-- Триггеры time_tracking для проверки состояния задачи

CREATE OR REPLACE FUNCTION fnc_check_time_validity()
    RETURNS TRIGGER AS
    $$
        DECLARE
            last_state INT;
            last_time TIME;
        BEGIN
            SELECT state, time into last_state, last_time
            FROM time_tracking
            WHERE name_peers = NEW.name_peers
                AND date = NEW.date
            ORDER BY time DESC
            LIMIT 1;

            IF last_state IS NULL AND NEW.state = 2 THEN
                RAISE NOTICE 'ID %: Пропуск: первый статус дня для пира "%" не может быть "выход" (2).',
                    NEW.pk_time_tracking, NEW.name_peers ;
                RETURN NULL;
            END IF;

            IF last_state = NEW.state THEN
                RAISE NOTICE 'ID %: Пропуск: повтор статуса "%" для пира "%" в дату "%".',
                    NEW.pk_time_tracking, NEW.state, NEW.name_peers, NEW.date;
                RETURN NULL;
            END IF;

            IF last_time IS NOT NULL AND NEW.time <= last_time THEN
                RAISE NOTICE 'ID %: Пропуск: время "%" для пира "%" в дату "%" меньше предыдущего "%".',
                    NEW.pk_time_tracking, NEW.time, NEW.name_peers, NEW.date, last_time;
                RETURN NULL;
            END IF;

            RETURN NEW;
        END;
    $$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_check_time_validity
    BEFORE INSERT OR UPDATE ON time_tracking
    FOR EACH ROW
    EXECUTE FUNCTION fnc_check_time_validity();

-- Процедуры для импорта/экспорта данных (CSV)
CREATE OR REPLACE PROCEDURE import_from_csv(
    table_name varchar,
    file_path varchar,
    delimiter char
)
    LANGUAGE plpgsql
    AS $$
BEGIN
    EXECUTE format(
        'COPY %I FROM %L WITH (FORMAT CSV, DELIMITER %L, HEADER true, NULL "None")',
        table_name,
        file_path,
        delimiter
    );
END;
$$;

CREATE OR REPLACE PROCEDURE export_to_csv(
    table_name varchar,
    file_path varchar,
    delimiter char
)
    LANGUAGE plpgsql
    AS $$
        BEGIN
            EXECUTE format('COPY %I TO %L WITH CSV DELIMITER %L;',
                table_name,
                file_path,
                delimiter);
        END;
    $$;


-- ИМПОРТ ДАТЫ
DO
$$
    DECLARE
        dataset_path VARCHAR := '/Users/yaroslavkuklin/Documents/Prog/21_projects/SQL/' ||
                                'SQL2_Info21_v1.0-1/src/datasets/';
        table_names TEXT[] := ARRAY[
            'peers', 'tasks', 'checks', 'p2p', 'verter', 'xp',
            'transferred_points', 'friends', 'recommendations', 'time_tracking'
        ]; tbl TEXT;
    BEGIN
        FOREACH tbl IN ARRAY table_names
            LOOP
                EXECUTE format('TRUNCATE TABLE %I RESTART IDENTITY CASCADE', tbl);
            END LOOP;
        SET datestyle TO 'DMY';
        FOREACH tbl IN ARRAY table_names
        LOOP
            BEGIN
                CALL import_from_csv(tbl,dataset_path || tbl || '.csv', ';');
            EXCEPTION
                WHEN OTHERS THEN
                    RAISE NOTICE 'Ошибка импорта таблицы %: %', tbl, SQLERRM;
            END;
        END LOOP;
    END;
$$;

-- Пример вызова процедуры экспорта в CSV
CALL export_to_csv('checks','/Users/yaroslavkuklin/Documents/Prog/21_projects/SQL/'' ||
                                ''SQL2_Info21_v1.0-1/src/checks.csv',
                   ';');