/* ============================================
 * part4.sql
 * ============================================
 * Проект: Info21
 * Участники команды:
 * - ullertyr (Team Leader) - Москва
 * - hbombur - Москва
 * - molliekh - Новосибирск
 * ============================================
 * Описание:
 * Этот файл предназначен для работы с метаданными
 * и объектами базы данных.
 * ============================================
 * Структура:
 * 1. Процедуры для удаления таблиц с заданным шаблоном
 * 2. Процедуры для анализа пользовательских функций
 * 3. Процедуры для удаления DML триггеров
 * 4. Процедуры для поиска объектов по строковому шаблону
 * ============================================
 * Примечание:
 * Включает тестовые вызовы процедур.
 * ============================================
 */

-- 1. Процедуры для удаления таблиц с заданным шаблоном
CREATE OR REPLACE PROCEDURE drop_tables_by_pattern(IN pattern VARCHAR)
    AS $$
        DECLARE
            table_name VARCHAR;
        BEGIN
            FOR table_name IN
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public'
                AND tablename LIKE (pattern || '%')
            LOOP
                EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', table_name);
                RAISE NOTICE 'Table % has been dropped', table_name;
                END LOOP;
        END;
    $$ LANGUAGE plpgsql;

call drop_tables_by_pattern('p');

-- select tablename
-- from pg_tables
-- where schemaname = 'public';


-- 2. Процедуры для анализа пользовательских функций
CREATE OR REPLACE PROCEDURE list_functions_with_params(OUT func_info VARCHAR, OUT count INT)
    AS $$
        DECLARE
        BEGIN
            select STRING_AGG(format('%s(%s)', proname, oidvectortypes(p.proargtypes)), E',\n '), count(*)
            INTO func_info, count
            from pg_proc p
            join pg_namespace on p.pronamespace = pg_namespace.oid
            where nspname = 'public'
            AND p.prorettype <> 'pg_catalog.trigger'::regtype
            AND p.prokind = 'f'
            AND p.pronargs > 0;
        END;

    $$ LANGUAGE plpgsql;

call list_functions_with_params(null, null);

-- 3. Процедуры для удаления DML триггеров
CREATE OR REPLACE PROCEDURE delete_all_bd_triggers (OUT deleted_triggers INT, OUT deleted_functions INT)
    AS $$
        DECLARE
            trigger_record RECORD;
            func_record RECORD;
        BEGIN
            deleted_triggers := 0;
            deleted_functions := 0;
            FOR trigger_record IN
                SELECT tgname, relname
                from pg_trigger pt
                JOIN pg_class pc ON pt.tgrelid = pc.oid
                WHERE NOT tgisinternal
            LOOP
                EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', trigger_record.tgname, trigger_record.relname);
                deleted_triggers := deleted_triggers + 1;
            END LOOP;
            FOR func_record IN
                SELECT oid::regprocedure AS function_name
                FROM pg_proc
                WHERE pronamespace = 'public'::regnamespace
                AND prokind = 'f'
                AND NOT EXISTS(SELECT 1
                               from pg_trigger
                               WHERE tgfoid = pg_proc.oid)
            LOOP
                EXECUTE format('DROP FUNCTION IF EXISTS %s CASCADE', func_record.function_name);
                deleted_functions := deleted_functions + 1;
                END LOOP;
        END;
    $$ LANGUAGE plpgsql;

call delete_all_bd_triggers(null, null);


-- 4. Процедуры для поиска объектов по строковому шаблону
CREATE OR REPLACE PROCEDURE find_objects_by_text (IN search_text VARCHAR)
    AS
        $$
            DECLARE
                object_name VARCHAR;
                object_type VARCHAR;
            BEGIN
                 FOR object_name, object_type IN
                    SELECT p.proname AS object_name,
                    CASE
                        WHEN p.prokind = 'p' THEN 'PROCEDURE'
                        ELSE 'FUNCTION'
                    END as object_type, p.prosrc AS object_definition
                    FROM pg_proc p
                    JOIN pg_namespace n ON p.pronamespace = n.oid
                    WHERE n.nspname = 'public'
                    AND prosrc ILIKE ('%' || search_text || '%')
                LOOP
                    RAISE NOTICE 'Object name: %, Object type: %', object_name, object_type;
                END LOOP;
            END;
       $$ LANGUAGE plpgsql;

CALL find_objects_by_text('FROM');

-- тесты
-- создания отдельной бд и схемы
create DATABASE test_db;
create schema test_schema;
drop schema test_schema cascade;

-- создание таблицы
CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    salary INT
);

-- триггер на вставку (INSERT)
CREATE OR REPLACE FUNCTION after_insert_employee()
RETURNS TRIGGER AS $$
BEGIN
    RAISE NOTICE 'Новая запись добавлена в таблицу employees: %, %', NEW.name, NEW.salary;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_after_insert_employee
AFTER INSERT ON test_schema.employees
FOR EACH ROW EXECUTE FUNCTION after_insert_employee();

-- триггер на обновление (UPDATE)
CREATE OR REPLACE FUNCTION after_update_employee()
RETURNS TRIGGER AS $$
BEGIN
    RAISE NOTICE 'Запись в таблице employees обновлена: %, %', NEW.name, NEW.salary;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_after_update_employee
AFTER UPDATE ON test_schema.employees
FOR EACH ROW EXECUTE FUNCTION after_update_employee();

-- триггер на удаление (DELETE)
CREATE OR REPLACE FUNCTION after_delete_employee()
RETURNS TRIGGER AS $$
BEGIN
    RAISE NOTICE 'Запись удалена из таблицы employees: %, %', OLD.name, OLD.salary;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_after_delete_employee
AFTER DELETE ON test_schema.employees
FOR EACH ROW EXECUTE FUNCTION after_delete_employee();

-- триггер на очистку таблицы (TRUNCATE)
CREATE OR REPLACE FUNCTION after_truncate_employee()
RETURNS TRIGGER AS $$
BEGIN
    RAISE NOTICE 'Таблица employees была очищена';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_after_truncate_employee
AFTER TRUNCATE ON test_schema.employees
FOR EACH STATEMENT EXECUTE FUNCTION after_truncate_employee();


