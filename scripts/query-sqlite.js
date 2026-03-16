/**
 * Exécution des requêtes analytiques sur la base SQLite locale
 * Equivalent offline de scripts/run-queries.js (PostgreSQL)
 *
 * Prérequis : avoir chargé la base SQLite (npm run load:sqlite)
 * Usage    : npm run query:sqlite
 *
 * Note SQLite vs PostgreSQL :
 *   - FILTER (WHERE ...) → supporté depuis SQLite 3.30 ✓
 *   - Window Functions   → supportées depuis SQLite 3.25 ✓
 *   - TO_CHAR/TO_DATE    → remplacé par CASE WHEN
 *   - DISTINCT ON        → remplacé par ROW_NUMBER()
 */

const fs   = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const DB_PATH    = path.join(OUTPUT_DIR, 'accidents.db');

// Vérifier que la base existe
if (!fs.existsSync(DB_PATH)) {
  console.error('\n  ✗ Base SQLite introuvable : output/accidents.db');
  console.error('  → Lancez d\'abord : npm run load:sqlite\n');
  process.exit(1);
}

const db = new Database(DB_PATH, { readonly: true });
if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

// ─────────────────────────────────────────────
// UTILITAIRES
// ─────────────────────────────────────────────

function afficher(titre, resultats) {
  console.log('\n' + '═'.repeat(70));
  console.log(`  ${titre}`);
  console.log('═'.repeat(70));
  if (!resultats || resultats.length === 0) {
    console.log('  Aucun résultat.');
    return;
  }
  console.table(resultats);
}

function sauvegarder(nomFichier, data) {
  const chemin = path.join(OUTPUT_DIR, nomFichier);
  fs.writeFileSync(chemin, JSON.stringify(data, null, 2), 'utf-8');
  console.log(`  → Sauvegardé : output/${nomFichier}`);
}

function requete(sql) {
  return db.prepare(sql).all();
}

// ─────────────────────────────────────────────
// REQUÊTE 1 : Victimes par sexe et gravité
// ─────────────────────────────────────────────
function req1() {
  const res = requete(`
    SELECT
      s.libelle                                                 AS sexe,
      g.libelle                                                 AS gravite,
      COUNT(*)                                                  AS nb_victimes,
      ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (PARTITION BY fu.id_sexe),
        2
      )                                                         AS pct_dans_sexe
    FROM fait_usagers fu
    JOIN dim_sexe    s ON fu.id_sexe    = s.id_sexe
    JOIN dim_gravite g ON fu.id_gravite = g.id_gravite
    GROUP BY fu.id_sexe, s.libelle, fu.id_gravite, g.libelle, g.niveau_severite
    ORDER BY fu.id_sexe, g.niveau_severite DESC
  `);
  afficher('REQUÊTE 1 — Victimes par sexe et gravité (avec %)', res);
  sauvegarder('sqlite_req1_victimes_sexe_gravite.json', res);
  return res;
}

// ─────────────────────────────────────────────
// REQUÊTE 2 : Taux de mortalité par sexe
// ─────────────────────────────────────────────
function req2() {
  const res = requete(`
    WITH stats_sexe AS (
      SELECT
        fu.id_sexe,
        s.libelle                                               AS sexe,
        COUNT(*)                                                AS total_victimes,
        COUNT(*) FILTER (WHERE fu.id_gravite = 2)              AS nb_tues,
        COUNT(*) FILTER (WHERE fu.id_gravite = 3)              AS nb_hospitalises,
        COUNT(*) FILTER (WHERE fu.id_gravite = 4)              AS nb_blesses_legers
      FROM fait_usagers fu
      JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
      GROUP BY fu.id_sexe, s.libelle
    )
    SELECT
      sexe,
      total_victimes,
      nb_tues,
      ROUND(nb_tues * 100.0 / MAX(total_victimes, 1), 2)      AS taux_mortalite_pct,
      ROUND(nb_tues * 100.0 / NULLIF(SUM(nb_tues) OVER (), 0), 1)
                                                                AS part_des_tues_pct
    FROM stats_sexe
    ORDER BY taux_mortalite_pct DESC
  `);
  afficher('REQUÊTE 2 — Taux de mortalité par sexe', res);
  sauvegarder('sqlite_req2_taux_mortalite.json', res);
  return res;
}

// ─────────────────────────────────────────────
// REQUÊTE 3 : Décès par tranche d'âge et sexe
// ─────────────────────────────────────────────
function req3() {
  const res = requete(`
    SELECT
      s.libelle                                                 AS sexe,
      CASE
        WHEN fu.age_au_moment < 18              THEN 'Moins de 18 ans'
        WHEN fu.age_au_moment BETWEEN 18 AND 25 THEN '18-25 ans'
        WHEN fu.age_au_moment BETWEEN 26 AND 35 THEN '26-35 ans'
        WHEN fu.age_au_moment BETWEEN 36 AND 50 THEN '36-50 ans'
        WHEN fu.age_au_moment BETWEEN 51 AND 65 THEN '51-65 ans'
        WHEN fu.age_au_moment > 65              THEN 'Plus de 65 ans'
        ELSE 'Inconnu'
      END                                                       AS tranche_age,
      COUNT(*)                                                  AS total,
      COUNT(*) FILTER (WHERE fu.id_gravite = 2)                AS nb_tues,
      ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        MAX(COUNT(*), 1), 2
      )                                                         AS taux_mortalite_pct
    FROM fait_usagers fu
    JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
    WHERE fu.age_au_moment IS NOT NULL
    GROUP BY fu.id_sexe, s.libelle, tranche_age
    ORDER BY fu.id_sexe, MIN(fu.age_au_moment)
  `);
  afficher('REQUÊTE 3 — Décès par tranche d\'âge et sexe', res);
  sauvegarder('sqlite_req3_deces_age.json', res);
  return res;
}

// ─────────────────────────────────────────────
// REQUÊTE 4 : Décès par luminosité et sexe
// ─────────────────────────────────────────────
function req4() {
  const res = requete(`
    SELECT
      s.libelle                                                 AS sexe,
      l.libelle                                                 AS luminosite,
      COUNT(fu.id_usager)                                       AS total,
      COUNT(*) FILTER (WHERE fu.id_gravite = 2)                AS nb_tues,
      ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        MAX(COUNT(fu.id_usager), 1), 2
      )                                                         AS taux_mortalite_pct
    FROM fait_usagers fu
    JOIN dim_sexe       s  ON fu.id_sexe       = s.id_sexe
    JOIN dim_accident   da ON fu.num_acc       = da.num_acc
    JOIN dim_luminosite l  ON da.id_luminosite = l.id_luminosite
    GROUP BY fu.id_sexe, s.libelle, da.id_luminosite, l.libelle
    HAVING COUNT(fu.id_usager) > 50
    ORDER BY fu.id_sexe, taux_mortalite_pct DESC
  `);
  afficher('REQUÊTE 4 — Décès par luminosité et sexe', res);
  sauvegarder('sqlite_req4_deces_luminosite.json', res);
  return res;
}

// ─────────────────────────────────────────────
// REQUÊTE 5 : Saisonnalité par mois et sexe
// (TO_CHAR/TO_DATE non disponible en SQLite → CASE WHEN)
// ─────────────────────────────────────────────
function req5() {
  const res = requete(`
    SELECT
      s.libelle                                                 AS sexe,
      dt.mois,
      CASE dt.mois
        WHEN 1  THEN 'Janvier'   WHEN 2  THEN 'Février'
        WHEN 3  THEN 'Mars'      WHEN 4  THEN 'Avril'
        WHEN 5  THEN 'Mai'       WHEN 6  THEN 'Juin'
        WHEN 7  THEN 'Juillet'   WHEN 8  THEN 'Août'
        WHEN 9  THEN 'Septembre' WHEN 10 THEN 'Octobre'
        WHEN 11 THEN 'Novembre'  WHEN 12 THEN 'Décembre'
        ELSE CAST(dt.mois AS TEXT)
      END                                                       AS nom_mois,
      COUNT(fu.id_usager)                                       AS total_impliques,
      COUNT(*) FILTER (WHERE fu.id_gravite = 2)                AS nb_tues
    FROM fait_usagers fu
    JOIN dim_sexe     s  ON fu.id_sexe  = s.id_sexe
    JOIN dim_accident da ON fu.num_acc  = da.num_acc
    JOIN dim_temps    dt ON da.id_temps = dt.id_temps
    WHERE dt.mois IS NOT NULL
    GROUP BY fu.id_sexe, s.libelle, dt.mois
    ORDER BY fu.id_sexe, dt.mois
  `);
  afficher('REQUÊTE 5 — Saisonnalité des décès par mois et sexe', res);
  sauvegarder('sqlite_req5_saisonnalite.json', res);
  return res;
}

// ─────────────────────────────────────────────
// REQUÊTE 6 : Gravité par rôle et sexe
// ─────────────────────────────────────────────
function req6() {
  const res = requete(`
    SELECT
      s.libelle                                                 AS sexe,
      CASE fu.categorie_usager
        WHEN 1 THEN 'Conducteur'
        WHEN 2 THEN 'Passager'
        WHEN 3 THEN 'Piéton'
        ELSE 'Autre'
      END                                                       AS role,
      COUNT(*)                                                  AS total,
      COUNT(*) FILTER (WHERE fu.id_gravite = 2)                AS nb_tues,
      ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        MAX(COUNT(*), 1), 2
      )                                                         AS taux_mortalite_pct
    FROM fait_usagers fu
    JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
    WHERE fu.categorie_usager IN (1, 2, 3)
    GROUP BY fu.id_sexe, s.libelle, fu.categorie_usager
    ORDER BY fu.id_sexe, taux_mortalite_pct DESC
  `);
  afficher('REQUÊTE 6 — Mortalité par rôle (conducteur/passager/piéton) et sexe', res);
  sauvegarder('sqlite_req6_role_sexe.json', res);
  return res;
}

// ─────────────────────────────────────────────
// REQUÊTE 7 : Top véhicules par sexe
// ─────────────────────────────────────────────
function req7() {
  const res = requete(`
    WITH stats AS (
      SELECT
        s.libelle                                               AS sexe,
        cv.libelle                                              AS type_vehicule,
        COUNT(fu.id_usager)                                     AS total,
        COUNT(*) FILTER (WHERE fu.id_gravite = 2)              AS nb_tues,
        RANK() OVER (
          PARTITION BY fu.id_sexe
          ORDER BY COUNT(fu.id_usager) DESC
        )                                                       AS rang
      FROM fait_usagers fu
      JOIN dim_sexe               s  ON fu.id_sexe      = s.id_sexe
      JOIN dim_vehicule           dv ON fu.id_vehicule  = dv.id_vehicule
      JOIN dim_categorie_vehicule cv ON dv.id_categorie = cv.id_categorie
      GROUP BY fu.id_sexe, s.libelle, dv.id_categorie, cv.libelle
    )
    SELECT sexe, rang, type_vehicule, total, nb_tues
    FROM stats
    WHERE rang <= 5
    ORDER BY sexe, rang
  `);
  afficher('REQUÊTE 7 — Top 5 types de véhicules par sexe', res);
  sauvegarder('sqlite_req7_vehicules.json', res);
  return res;
}

// ─────────────────────────────────────────────
// REQUÊTE 8 : Vue récapitulative complète
// (DISTINCT ON PostgreSQL → ROW_NUMBER en SQLite)
// ─────────────────────────────────────────────
function req8() {
  const res = requete(`
    WITH
    base AS (
      SELECT
        fu.id_sexe,
        s.libelle                                               AS sexe,
        COUNT(*)                                                AS total_impliques,
        COUNT(*) FILTER (WHERE fu.id_gravite = 2)              AS nb_tues,
        COUNT(*) FILTER (WHERE fu.id_gravite = 3)              AS nb_hospitalises,
        COUNT(*) FILTER (WHERE fu.id_gravite = 4)              AS nb_blesses_legers,
        ROUND(AVG(fu.age_au_moment), 1)                        AS age_moyen,
        COUNT(DISTINCT fu.num_acc)                             AS nb_accidents_distincts
      FROM fait_usagers fu
      JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
      GROUP BY fu.id_sexe, s.libelle
    ),
    totaux AS (
      SELECT
        SUM(total_impliques) AS grand_total,
        SUM(nb_tues)         AS total_tues
      FROM base
    )
    SELECT
      b.sexe,
      b.total_impliques,
      ROUND(b.total_impliques * 100.0 / t.grand_total, 1)     AS pct_du_total,
      b.nb_tues,
      ROUND(b.nb_tues * 100.0 / MAX(b.total_impliques, 1), 2) AS taux_mortalite_pct,
      ROUND(b.nb_tues * 100.0 / MAX(t.total_tues, 1), 1)      AS part_tues_pct,
      b.nb_hospitalises,
      b.nb_blesses_legers,
      b.age_moyen,
      b.nb_accidents_distincts
    FROM base b
    CROSS JOIN totaux t
    ORDER BY b.id_sexe
  `);
  afficher('REQUÊTE 8 — Vue récapitulative complète', res);
  sauvegarder('sqlite_req8_recap_complet.json', res);
  return res;
}

// ─────────────────────────────────────────────
// REQUÊTE 9 : Évolution du taux de mortalité par année et sexe
// (nécessite des données multi-années : npm run etl:full)
// ─────────────────────────────────────────────
function req9() {
  const res = requete(`
    SELECT
      s.libelle                                                 AS sexe,
      fu.annee_donnees                                          AS annee,
      COUNT(*)                                                  AS total_victimes,
      COUNT(*) FILTER (WHERE fu.id_gravite = 2)                AS nb_tues,
      ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        MAX(COUNT(*), 1), 2
      )                                                         AS taux_mortalite_pct,
      ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        NULLIF(SUM(COUNT(*) FILTER (WHERE fu.id_gravite = 2)) OVER (PARTITION BY fu.annee_donnees), 0),
        1
      )                                                         AS part_tues_dans_annee_pct
    FROM fait_usagers fu
    JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
    WHERE fu.annee_donnees IS NOT NULL
    GROUP BY fu.id_sexe, s.libelle, fu.annee_donnees
    ORDER BY fu.annee_donnees, fu.id_sexe
  `);
  afficher('REQUÊTE 9 — Évolution du taux de mortalité par année et sexe', res);
  sauvegarder('sqlite_req9_evolution_annee.json', res);
  return res;
}

// ─────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────

function main() {
  console.log('\n' + '█'.repeat(70));
  console.log('  ANALYSE SQLITE — Accidents de la Route France — Mortalité par sexe');
  console.log('  Base : output/accidents.db (mode offline)');
  console.log('█'.repeat(70));

  try {
    // Vérifier que les tables existent et ont des données
    const count = db.prepare('SELECT COUNT(*) AS n FROM fait_usagers').get();
    console.log(`  ✓ Base SQLite OK — ${count.n.toLocaleString('fr-FR')} usagers chargés`);

    req1();
    req2();
    req3();
    req4();
    req5();
    req6();
    req7();
    req8();
    req9();

    console.log('\n' + '═'.repeat(70));
    console.log('  ✓ Toutes les analyses terminées. Résultats dans output/sqlite_*.json');
    console.log('═'.repeat(70) + '\n');

  } catch (err) {
    console.error('\n  ✗ ERREUR :', err.message);
    if (err.message.includes('no such table')) {
      console.error('  → La base est vide. Lancez d\'abord : npm run load:sqlite');
    }
    process.exit(1);
  } finally {
    db.close();
  }
}

main();
