  -- Ce script SQL est une procédure stockée nommée ddl_bonze.
  -- Son objectif est de créer toutes les tables de la "zone bronze" (brute)
  -- d'un data warehouse.
  --
  -- Dans la zone bronze, les tables sont conçues pour l'ingestion rapide
  -- des données brutes, sans contraintes de clés primaires ou étrangères,
  -- ni d'index. Les types de données sont ajustés pour gérer les variations
  -- de la source.
  --
  -- L'utilisation d'un seul bloc EXECUTE améliore la performance en réduisant
  -- les allers-retours avec le serveur de base de données.

CREATE OR REPLACE PROCEDURE ddl_bonze()
LANGUAGE plpgsql
AS $$
BEGIN

    EXECUTE '
        -- Création de la table individuals : Contient des informations de base sur les individus.
        CREATE TABLE IF NOT EXISTS bronze.individuals (
            uuid VARCHAR(100),
            extId VARCHAR(50),
            dob DATE,
            firstName VARCHAR(100),
            middleName VARCHAR(100),
            lastName VARCHAR(100),
            gender VARCHAR(10),
            religion VARCHAR(50),
            father_uuid VARCHAR(100),
            mother_uuid VARCHAR(100),
            insertDate DATE
        );

        -- Création de la table death : Enregistre les informations sur les décès.
        CREATE TABLE IF NOT EXISTS bronze.death (
            uuid VARCHAR(200),
            insertDate DATE,
            ageAtDeath VARCHAR(100), -- Le type est VARCHAR car les données brutes peuvent être incohérentes.
            deathCause VARCHAR(100),
            deathDate VARCHAR(100),   -- Le type est VARCHAR pour gérer les différents formats de date de la source.
            deathPlace VARCHAR(100),
            individual_uuid VARCHAR(100),
            visitDeath_uuid VARCHAR(100)
        );

        -- Création de la table inmigration : Enregistre les événements d''immigration.
        CREATE TABLE IF NOT EXISTS bronze.inmigration (
            uuid VARCHAR(100),
            origin VARCHAR(100),
            migType VARCHAR(100),
            reason VARCHAR(100),
            insertDate DATE,
            recordedDate DATE,
            individual_uuid VARCHAR(100),
            residency_uuid VARCHAR(100)
        );

        -- Création de la table location : Stocke les données de localisation.
        CREATE TABLE IF NOT EXISTS bronze.location (
            uuid VARCHAR(100),
            extId VARCHAR(100),
            locationName VARCHAR(100),
            locationtype VARCHAR(10),
            accuracy VARCHAR(100),
            altitude VARCHAR(100),
            latitude VARCHAR(100),
            longitude VARCHAR(100),
            insertDate DATE
        );

        -- Création de la table locationhierarchy : Décrit la hiérarchie des lieux.
        CREATE TABLE IF NOT EXISTS bronze.locationhierarchy (
            uuid VARCHAR(100),
            extId VARCHAR(100),
            name VARCHAR(100),
            level_uuid VARCHAR(100),
            parent_uuid VARCHAR(100)
        );

        -- Création de la table locationhierarchylevel : Définit les niveaux dans la hiérarchie des lieux.
        CREATE TABLE IF NOT EXISTS bronze.locationhierarchylevel (
            uuid VARCHAR(100),
            keyIdentifier VARCHAR(100),
            name VARCHAR(100)
        );

        -- Création de la table pregnancyoutcome_outcome : Table de liaison pour les résultats de grossesse.
        CREATE TABLE IF NOT EXISTS bronze.pregnancyoutcome_outcome (
            pregnancyoutcome_uuid VARCHAR(200),
            outcomes_uuid VARCHAR(100)
        );

        -- Création de la table outmigration : Enregistre les événements d''émigration.
        CREATE TABLE IF NOT EXISTS bronze.outmigration (
            uuid VARCHAR(100),
            destination VARCHAR(100),
            reason VARCHAR(100),
            insertDate DATE,
            recordedDate DATE,
            individual_uuid VARCHAR(100),
            residency_uuid VARCHAR(100)
        );

        -- Création de la table pregnancyobservation : Enregistre les observations de grossesse.
        CREATE TABLE IF NOT EXISTS bronze.pregnancyobservation (
            uuid VARCHAR(100),
            recordedDate DATE,
            expectedDeliveryDate DATE,
            insertDate DATE,
            mother_uuid VARCHAR(100)
        );

        -- Création de la table pregnancyoutcome : Enregistre les résultats des grossesses.
        CREATE TABLE IF NOT EXISTS bronze.pregnancyoutcome (
            uuid VARCHAR(100),
            childEverBorn VARCHAR(10),
            numberOfLiveBirths VARCHAR(10),
            outcomeDate DATE,
            insertDate DATE,
            father_uuid VARCHAR(100),
            mother_uuid VARCHAR(100),
            visit_uuid VARCHAR(100)
        );

        -- Création de la table relationship : Enregistre les relations entre individus.
        CREATE TABLE IF NOT EXISTS bronze.relationship (
            uuid VARCHAR(100),
            aIsTob VARCHAR(100),
            startDate DATE,
            endDate DATE,
            endType VARCHAR(10),
            insertDate DATE,
            individualA_uuid VARCHAR(100),
            individualB_uuid VARCHAR(100)
        );

        -- Création de la table residency : Enregistre les informations de résidence des individus.
        CREATE TABLE IF NOT EXISTS bronze.residency (
            uuid VARCHAR(100),
            startType VARCHAR(10),
            startDate DATE,
            endType VARCHAR(10),
            endDate DATE,
            insertDate DATE,
            individual_uuid VARCHAR(100),
            location_uuid VARCHAR(100)
        );

        -- Création de la table socialgroup : Enregistre les informations sur les groupes sociaux.
        CREATE TABLE IF NOT EXISTS bronze.socialgroup (
            uuid VARCHAR(100),
            extId VARCHAR(100),
            groupName VARCHAR(100),
            groupType VARCHAR(10),
            insertDate DATE,
            groupHead_uuid VARCHAR(100)
        );

        -- Création de la table outcome : Enregistre les résultats d''une grossesse.
        CREATE TABLE IF NOT EXISTS bronze.outcome (
            uuid VARCHAR(200),
            type VARCHAR(100),
            child_uuid VARCHAR(100),
            childMembership_uuid VARCHAR(100),
            childextId VARCHAR(100)
        );
    ';
END;
$$;
