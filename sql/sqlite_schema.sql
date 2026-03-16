-- ============================================================
-- SCHÉMA SQLITE — Accidents de la Route en France
-- Adapté depuis le schéma PostgreSQL (01_schema.sql)
-- Différences principales :
--   SERIAL         → INTEGER PRIMARY KEY
--   VARCHAR(n)     → TEXT
--   SMALLINT       → INTEGER
--   DECIMAL(10,6)  → REAL
--   CASCADE        → non supporté, supprimé
-- ============================================================


-- ============================================================
-- NETTOYAGE (ordre inverse des dépendances)
-- ============================================================

DROP TABLE IF EXISTS fait_usagers;
DROP TABLE IF EXISTS dim_vehicule;
DROP TABLE IF EXISTS dim_accident;
DROP TABLE IF EXISTS dim_lieu;
DROP TABLE IF EXISTS dim_temps;
DROP TABLE IF EXISTS dim_categorie_vehicule;
DROP TABLE IF EXISTS dim_atmosphere;
DROP TABLE IF EXISTS dim_luminosite;
DROP TABLE IF EXISTS dim_gravite;
DROP TABLE IF EXISTS dim_sexe;


-- ============================================================
-- TABLES DE DIMENSIONS
-- ============================================================

CREATE TABLE dim_sexe (
    id_sexe   INTEGER PRIMARY KEY,
    libelle   TEXT    NOT NULL
);

CREATE TABLE dim_gravite (
    id_gravite      INTEGER PRIMARY KEY,
    libelle         TEXT    NOT NULL,
    niveau_severite INTEGER NOT NULL
);

CREATE TABLE dim_luminosite (
    id_luminosite INTEGER PRIMARY KEY,
    libelle       TEXT    NOT NULL
);

CREATE TABLE dim_atmosphere (
    id_atmosphere INTEGER PRIMARY KEY,
    libelle       TEXT    NOT NULL
);

CREATE TABLE dim_categorie_vehicule (
    id_categorie INTEGER PRIMARY KEY,
    libelle      TEXT    NOT NULL
);

-- 1 ligne par accident (données temporelles)
CREATE TABLE dim_temps (
    id_temps        INTEGER PRIMARY KEY,
    jour            INTEGER,
    mois            INTEGER,
    annee           INTEGER,
    heure           INTEGER,
    minute          INTEGER,
    trimestre       INTEGER,
    tranche_horaire TEXT
);

-- 1 ligne par accident (données géographiques)
CREATE TABLE dim_lieu (
    id_lieu       INTEGER PRIMARY KEY,
    departement   TEXT,
    commune       TEXT,
    agglomeration INTEGER,
    adresse       TEXT,
    latitude      REAL,
    longitude     REAL
);

-- 1 ligne par accident (table centrale)
CREATE TABLE dim_accident (
    num_acc           TEXT    PRIMARY KEY,
    id_temps          INTEGER,
    id_lieu           INTEGER,
    id_luminosite     INTEGER,
    id_atmosphere     INTEGER,
    type_intersection INTEGER,
    type_collision    INTEGER
);

-- 1 ligne par véhicule impliqué
CREATE TABLE dim_vehicule (
    id_vehicule  INTEGER PRIMARY KEY,
    num_acc      TEXT,
    num_veh      TEXT,
    id_categorie INTEGER,
    sens         INTEGER,
    obstacle     INTEGER,
    choc         INTEGER,
    manoeuvre    INTEGER
);


-- ============================================================
-- TABLE DE FAITS
-- ============================================================

-- 1 ligne par usager impliqué dans un accident
CREATE TABLE fait_usagers (
    id_usager           INTEGER PRIMARY KEY,
    num_acc             TEXT,
    id_vehicule         INTEGER,
    id_sexe             INTEGER NOT NULL,
    id_gravite          INTEGER NOT NULL,
    place               INTEGER,
    categorie_usager    INTEGER,   -- 1=conducteur, 2=passager, 3=piéton
    annee_naissance     INTEGER,
    age_au_moment       INTEGER,
    type_trajet         INTEGER,
    equipement_securite INTEGER,
    annee_donnees       INTEGER    -- année source des données (2012-2022)
);


-- ============================================================
-- INDEX
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_fait_sexe         ON fait_usagers(id_sexe);
CREATE INDEX IF NOT EXISTS idx_fait_gravite       ON fait_usagers(id_gravite);
CREATE INDEX IF NOT EXISTS idx_fait_num_acc       ON fait_usagers(num_acc);
CREATE INDEX IF NOT EXISTS idx_fait_age           ON fait_usagers(age_au_moment);
CREATE INDEX IF NOT EXISTS idx_fait_categorie     ON fait_usagers(categorie_usager);
CREATE INDEX IF NOT EXISTS idx_fait_sexe_gravite  ON fait_usagers(id_sexe, id_gravite);
CREATE INDEX IF NOT EXISTS idx_fait_annee         ON fait_usagers(annee_donnees);
CREATE INDEX IF NOT EXISTS idx_temps_mois         ON dim_temps(mois);
CREATE INDEX IF NOT EXISTS idx_temps_annee        ON dim_temps(annee);
CREATE INDEX IF NOT EXISTS idx_temps_tranche      ON dim_temps(tranche_horaire);
CREATE INDEX IF NOT EXISTS idx_lieu_dept          ON dim_lieu(departement);
