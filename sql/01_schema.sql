-- ============================================================
-- SCHÉMA STAR SCHEMA — Accidents de la Route en France 2022
-- Projet ETL — Web School Factory — SQL 4 Big Data
-- Thématique : Mortalité comparée par sexe
-- ============================================================


-- ============================================================
-- NETTOYAGE (drop dans l'ordre inverse des dépendances FK)
-- ============================================================

DROP TABLE IF EXISTS fait_usagers CASCADE;
DROP TABLE IF EXISTS dim_vehicule CASCADE;
DROP TABLE IF EXISTS dim_accident CASCADE;
DROP TABLE IF EXISTS dim_lieu CASCADE;
DROP TABLE IF EXISTS dim_temps CASCADE;
DROP TABLE IF EXISTS dim_categorie_vehicule CASCADE;
DROP TABLE IF EXISTS dim_atmosphere CASCADE;
DROP TABLE IF EXISTS dim_luminosite CASCADE;
DROP TABLE IF EXISTS dim_gravite CASCADE;
DROP TABLE IF EXISTS dim_sexe CASCADE;


-- ============================================================
-- TABLES DE DIMENSIONS
-- ============================================================

-- Dimension Sexe (2 valeurs : Masculin / Féminin)
CREATE TABLE dim_sexe (
    id_sexe     SMALLINT    PRIMARY KEY,
    libelle     VARCHAR(20) NOT NULL
);

-- Dimension Gravité de l'accident pour l'usager
-- 1=Indemne, 2=Tué, 3=Hospitalisé, 4=Blessé léger
CREATE TABLE dim_gravite (
    id_gravite      SMALLINT    PRIMARY KEY,
    libelle         VARCHAR(30) NOT NULL,
    niveau_severite SMALLINT    NOT NULL  -- 0=aucun → 3=fatal
);

-- Dimension Luminosité au moment de l'accident
CREATE TABLE dim_luminosite (
    id_luminosite   SMALLINT    PRIMARY KEY,
    libelle         VARCHAR(60) NOT NULL
);

-- Dimension Conditions Atmosphériques
CREATE TABLE dim_atmosphere (
    id_atmosphere   SMALLINT    PRIMARY KEY,
    libelle         VARCHAR(40) NOT NULL
);

-- Dimension Catégorie de Véhicule
CREATE TABLE dim_categorie_vehicule (
    id_categorie    SMALLINT    PRIMARY KEY,
    libelle         VARCHAR(60) NOT NULL
);

-- Dimension Temps (1 ligne par accident)
CREATE TABLE dim_temps (
    id_temps        SERIAL      PRIMARY KEY,
    jour            SMALLINT,
    mois            SMALLINT,
    annee           SMALLINT,
    heure           SMALLINT,
    minute          SMALLINT,
    trimestre       SMALLINT,                    -- 1 à 4
    tranche_horaire VARCHAR(15)                  -- matin/apres-midi/soiree/nuit
);

-- Dimension Lieu (1 ligne par accident, coordonnées géographiques)
CREATE TABLE dim_lieu (
    id_lieu         SERIAL          PRIMARY KEY,
    departement     VARCHAR(5),
    commune         VARCHAR(10),
    agglomeration   SMALLINT,                    -- 1=hors agglo, 2=en agglo
    adresse         VARCHAR(500),
    latitude        DECIMAL(10, 6),
    longitude       DECIMAL(10, 6)
);

-- Dimension Accident (table centrale - 1 ligne par accident)
-- FK retirées sur luminosite/atmosphere : les données réelles contiennent
-- des codes hors-référentiel (ex: 0) qui causeraient des violations FK
CREATE TABLE dim_accident (
    num_acc             VARCHAR(20)  PRIMARY KEY,
    id_temps            INTEGER,
    id_lieu             INTEGER,
    id_luminosite       SMALLINT,
    id_atmosphere       SMALLINT,
    type_intersection   SMALLINT,
    type_collision      SMALLINT
);

-- Dimension Véhicule (1 ligne par véhicule impliqué)
CREATE TABLE dim_vehicule (
    id_vehicule     SERIAL       PRIMARY KEY,
    num_acc         VARCHAR(20),
    num_veh         VARCHAR(10),
    id_categorie    SMALLINT,
    sens            SMALLINT,
    obstacle        SMALLINT,
    choc            SMALLINT,
    manoeuvre       SMALLINT
);


-- ============================================================
-- TABLE DE FAITS
-- ============================================================

-- Fait Usagers (granularité : 1 ligne par usager impliqué)
-- C'est ici que se concentrent les analyses sexe × gravité
CREATE TABLE fait_usagers (
    id_usager           SERIAL      PRIMARY KEY,
    num_acc             VARCHAR(20),
    id_vehicule         INTEGER,
    id_sexe             SMALLINT    NOT NULL,
    id_gravite          SMALLINT    NOT NULL,
    place               SMALLINT,   -- place occupée dans le véhicule
    categorie_usager    SMALLINT,   -- 1=conducteur, 2=passager, 3=piéton
    annee_naissance     SMALLINT,
    age_au_moment       SMALLINT,   -- calculé lors de la transformation
    type_trajet         SMALLINT,   -- 1=domicile-travail, 2=domicile-école, etc.
    equipement_securite SMALLINT    -- ceinture, casque, etc.
);


-- ============================================================
-- INDEX (optimisation des requêtes analytiques)
-- ============================================================

-- Index principaux pour les filtres et jointures fréquents
CREATE INDEX idx_fait_sexe       ON fait_usagers(id_sexe);
CREATE INDEX idx_fait_gravite    ON fait_usagers(id_gravite);
CREATE INDEX idx_fait_num_acc    ON fait_usagers(num_acc);
CREATE INDEX idx_fait_vehicule   ON fait_usagers(id_vehicule);
CREATE INDEX idx_fait_age        ON fait_usagers(age_au_moment);
CREATE INDEX idx_fait_categorie  ON fait_usagers(categorie_usager);

-- Index sur les dimensions temporelles (analyses saisonnières)
CREATE INDEX idx_temps_mois      ON dim_temps(mois);
CREATE INDEX idx_temps_annee     ON dim_temps(annee);
CREATE INDEX idx_temps_tranche   ON dim_temps(tranche_horaire);
CREATE INDEX idx_temps_trimestre ON dim_temps(trimestre);

-- Index sur les dimensions géographiques
CREATE INDEX idx_lieu_dept       ON dim_lieu(departement);

-- Index composites pour les requêtes croisées (sexe × gravité)
CREATE INDEX idx_fait_sexe_gravite ON fait_usagers(id_sexe, id_gravite);
