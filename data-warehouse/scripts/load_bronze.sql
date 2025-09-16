/*
=============================================================
Procédure : load_bronze()
=============================================================
Objectif :
    - Charger automatiquement les données brutes ("bronze layer") 
      à partir de fichiers CSV externes vers les tables PostgreSQL correspondantes.
    - Chaque exécution génère un identifiant de batch (UUID) pour tracer
      toutes les étapes dans la table de log `etl_log`.

Fonctionnement :
    1. Pour chaque table définie dans la liste (v_step),
       - Extraire nom du schéma, table et chemin du fichier CSV.
       - Vider la table avec TRUNCATE.
       - Charger les données via COPY (délimiteur ; , NULL explicite).
       - Mesurer le temps de chargement et compter le nombre de lignes.
       - Insérer un log dans `etl_log` avec le batch_id.
    2. En cas d’erreur, la procédure capture l’exception et logue l’échec
       (avec message d’erreur SQL et rows_loaded = 0).
    3. Des messages NOTICE sont affichés pour suivi en temps réel.

Logs générés :
    - Table : etl_log
    - Colonnes remplies :
        batch_id      : UUID unique pour ce run
        schema_name   : schéma de la table cible
        table_name    : table cible
        file_name     : chemin du fichier CSV chargé
        status        : SUCCESS / ERROR
        message       : "Loaded successfully" ou SQLERRM
        start_time    : horodatage début du chargement
        end_time      : horodatage fin du chargement
        duration      : durée du chargement
        rows_loaded   : nombre de lignes insérées

Pré-requis :
    - Le schéma "bronze" et les tables cibles existent déjà.
    - Les fichiers CSV sont accessibles par PostgreSQL 
      (chemin `/tmp/dss_dwh/...`).
    - Extension `pgcrypto` ou `uuid-ossp` installée pour gen_random_uuid().

Avertissement :
    - Cette procédure TRONQUE les tables avant de charger les données,
      ce qui efface tout contenu existant.
    - S’assurer que les fichiers CSV respectent bien les colonnes
      définies dans chaque table.
=============================================================
*/

CREATE OR REPLACE PROCEDURE load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp(); -- horodatage global du batch
    v_step_start TIMESTAMP;   -- début d’un step (table)
    v_step_end   TIMESTAMP;   -- fin d’un step
    v_step_duration INTERVAL; -- durée du step
    v_schema TEXT;            -- schéma cible
    v_table  TEXT;            -- table cible
    v_file   TEXT;            -- chemin du CSV source
    v_step   TEXT;            -- entrée actuelle (table:fichier)
    v_rowcount BIGINT;        -- nombre de lignes insérées
    v_batch_id UUID := gen_random_uuid(); -- identifiant unique du batch
BEGIN
    RAISE NOTICE '=== DÉBUT DU CHARGEMENT (batch: %, start: %)', v_batch_id, v_start_time;

    -- Boucle sur toutes les tables/fichiers à charger
    FOR v_step IN 
        SELECT unnest(ARRAY[
            'bronze.individuals:/tmp/dss_dwh/individual.csv',
            'bronze.death:/tmp/dss_dwh/death.csv',
            'bronze.inmigration:/tmp/dss_dwh/inmigration.csv',
            'bronze.location:/tmp/dss_dwh/location.csv',
            'bronze.locationhierarchy:/tmp/dss_dwh/locationhierarchy.csv',
            'bronze.locationhierarchylevel:/tmp/dss_dwh/locationhierarchylevel.csv',
            'bronze.pregnancyoutcome_outcome:/tmp/dss_dwh/pregnancyoutcome_outcome.csv',
            'bronze.outmigration:/tmp/dss_dwh/outmigration.csv',
            'bronze.pregnancyobservation:/tmp/dss_dwh/pregnancyobservation.csv',
            'bronze.pregnancyoutcome:/tmp/dss_dwh/pregnancyoutcome.csv',
            'bronze.relationship:/tmp/dss_dwh/relationship.csv',
            'bronze.residency:/tmp/dss_dwh/residency.csv',
            'bronze.socialgroup:/tmp/dss_dwh/socialgroup.csv',
            'bronze.outcome:/tmp/dss_dwh/outcome.csv'
        ])
    LOOP
        BEGIN
            -- Décomposition de l’entrée en (schéma, table, fichier)
            v_schema := split_part(split_part(v_step, ':', 1), '.', 1);
            v_table  := split_part(split_part(v_step, ':', 1), '.', 2);
            v_file   := split_part(v_step, ':', 2);

            v_step_start := clock_timestamp();

            -- 1) Vider la table avant chargement
            EXECUTE format('TRUNCATE TABLE %I.%I', v_schema, v_table);

            -- 2) Importer depuis le CSV avec COPY
            EXECUTE format(
                'COPY %I.%I FROM %L DELIMITER %L CSV HEADER NULL %L',
                v_schema, v_table,
                v_file,
                ';',
                'NULL'
            );

            -- 3) Récupérer nombre de lignes chargées
            GET DIAGNOSTICS v_rowcount = ROW_COUNT;

            v_step_end := clock_timestamp();
            v_step_duration := v_step_end - v_step_start;

            -- 4) Log succès
            INSERT INTO etl_log(batch_id,schema_name, table_name, file_name, status, message, start_time, end_time, duration, rows_loaded)
            VALUES (v_batch_id,v_schema, v_table, v_file, 'SUCCESS', 'Loaded successfully', v_step_start, v_step_end, v_step_duration, v_rowcount);

            RAISE NOTICE '✅ %.% chargé (% lignes) en %', v_schema, v_table, v_rowcount, v_step_duration;

        EXCEPTION WHEN OTHERS THEN
            v_step_end := clock_timestamp();
            v_step_duration := v_step_end - v_step_start;

            -- 5) Log erreur
            INSERT INTO etl_log(batch_id,schema_name, table_name, file_name, status, message, start_time, end_time, duration, rows_loaded)
            VALUES (v_batch_id,v_schema, v_table, v_file, 'ERROR', SQLERRM, v_step_start, v_step_end, v_step_duration, 0);

            RAISE NOTICE '❌ ERREUR %.% : % (durée %)', v_schema, v_table, SQLERRM, v_step_duration;
        END;
    END LOOP;

    -- Résumé global
    RAISE NOTICE '=== FIN DU CHARGEMENT (batch: %, durée totale: %)', v_batch_id, clock_timestamp() - v_start_time;
END;
$$;
