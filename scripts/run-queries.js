/**
 * Script d'exécution des requêtes analytiques
 * Lance les 8 requêtes SQL et affiche les résultats dans le terminal
 * Exporte également les résultats en JSON dans le dossier output/
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const knex = require('knex');

const OUTPUT_DIR = path.join(__dirname, '..', 'output');

// Connexion à la base de données
const db = knex({
  client: 'pg',
  connection: {
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  }
});

// S'assurer que le dossier output existe
if (!fs.existsSync(OUTPUT_DIR)) {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

/** Affiche un tableau de résultats de manière lisible dans le terminal */
function afficherResultats(titre, resultats) {
  console.log('\n' + '═'.repeat(70));
  console.log(`  ${titre}`);
  console.log('═'.repeat(70));
  if (resultats.length === 0) {
    console.log('  Aucun résultat.');
    return;
  }
  console.table(resultats);
}

/** Affiche le tableau des régions avec colonnes alignées et lisibles */
function afficherRegions(rows) {
  console.log('\n' + '═'.repeat(70));
  console.log('  REQUÊTE 14 — Comparaison par région (mortalité H/F)');
  console.log('═'.repeat(70));
  if (rows.length === 0) { console.log('  Aucun résultat.'); return; }

  const pad = (s, n) => String(s).padEnd(n);
  const padL = (s, n) => String(s).padStart(n);

  const SEP = '─'.repeat(118);
  const header = [
    pad('Région', 30),
    padL('Victimes', 9),
    padL('Hommes', 8),
    padL('Femmes', 8),
    padL('Tués', 6),
    padL('Tués H', 7),
    padL('Tués F', 7),
    padL('Mort.%', 7),
    padL('Mort.H%', 8),
    padL('Mort.F%', 8),
  ].join('  ');

  console.log('\n  ' + header);
  console.log('  ' + SEP);

  for (const r of rows) {
    const ligne = [
      pad(r.region, 30),
      padL(r.total_victimes, 9),
      padL(r.victimes_hommes, 8),
      padL(r.victimes_femmes, 8),
      padL(r.nb_tues, 6),
      padL(r.tues_hommes, 7),
      padL(r.tues_femmes, 7),
      padL(r.taux_mortalite_pct != null ? r.taux_mortalite_pct + '%' : '-', 7),
      padL(r.taux_mortalite_hommes_pct != null ? r.taux_mortalite_hommes_pct + '%' : '-', 8),
      padL(r.taux_mortalite_femmes_pct != null ? r.taux_mortalite_femmes_pct + '%' : '-', 8),
    ].join('  ');
    console.log('  ' + ligne);
  }

  console.log('  ' + SEP);
  console.log('');
}

/** Sauvegarde les résultats en JSON dans le dossier output/ */
function sauvegarderJson(nomFichier, data) {
  const chemin = path.join(OUTPUT_DIR, nomFichier);
  fs.writeFileSync(chemin, JSON.stringify(data, null, 2), 'utf-8');
  console.log(`  → Sauvegardé : output/${nomFichier}`);
}

// ─────────────────────────────────────────────
// REQUÊTE 1 : Victimes par sexe et gravité
// ─────────────────────────────────────────────
async function requete1() {
  const resultats = await db.raw(`
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
  afficherResultats('REQUÊTE 1 — Victimes par sexe et gravité (avec %)', resultats.rows);
  sauvegarderJson('req1_victimes_sexe_gravite.json', resultats.rows);
  return resultats.rows;
}

// ─────────────────────────────────────────────
// REQUÊTE 2 : Taux de mortalité par sexe
// ─────────────────────────────────────────────
async function requete2() {
  const resultats = await db.raw(`
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
      ROUND(nb_tues * 100.0 / NULLIF(total_victimes, 0), 2)   AS taux_mortalite_pct,
      ROUND(nb_tues * 100.0 / NULLIF(SUM(nb_tues) OVER (), 0), 1)
                                                                AS part_des_tues_pct
    FROM stats_sexe
    ORDER BY taux_mortalite_pct DESC
  `);
  afficherResultats('REQUÊTE 2 — Taux de mortalité par sexe', resultats.rows);
  sauvegarderJson('req2_taux_mortalite.json', resultats.rows);
  return resultats.rows;
}

// ─────────────────────────────────────────────
// REQUÊTE 3 : Décès par tranche d'âge et sexe
// ─────────────────────────────────────────────
async function requete3() {
  const resultats = await db.raw(`
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
        NULLIF(COUNT(*), 0), 2
      )                                                         AS taux_mortalite_pct
    FROM fait_usagers fu
    JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
    WHERE fu.age_au_moment IS NOT NULL
    GROUP BY fu.id_sexe, s.libelle, tranche_age
    ORDER BY fu.id_sexe, MIN(fu.age_au_moment)
  `);
  afficherResultats('REQUÊTE 3 — Décès par tranche d\'âge et sexe', resultats.rows);
  sauvegarderJson('req3_deces_age.json', resultats.rows);
  return resultats.rows;
}

// ─────────────────────────────────────────────
// REQUÊTE 4 : Décès par luminosité et sexe
// ─────────────────────────────────────────────
async function requete4() {
  const resultats = await db.raw(`
    SELECT
      s.libelle                                                 AS sexe,
      l.libelle                                                 AS luminosite,
      COUNT(fu.id_usager)                                       AS total,
      COUNT(*) FILTER (WHERE fu.id_gravite = 2)                AS nb_tues,
      ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        NULLIF(COUNT(fu.id_usager), 0), 2
      )                                                         AS taux_mortalite_pct
    FROM fait_usagers fu
    JOIN dim_sexe       s  ON fu.id_sexe      = s.id_sexe
    JOIN dim_accident   da ON fu.num_acc      = da.num_acc
    JOIN dim_luminosite l  ON da.id_luminosite = l.id_luminosite
    GROUP BY fu.id_sexe, s.libelle, da.id_luminosite, l.libelle
    HAVING COUNT(fu.id_usager) > 50
    ORDER BY fu.id_sexe, taux_mortalite_pct DESC
  `);
  afficherResultats('REQUÊTE 4 — Décès par luminosité et sexe', resultats.rows);
  sauvegarderJson('req4_deces_luminosite.json', resultats.rows);
  return resultats.rows;
}

// ─────────────────────────────────────────────
// REQUÊTE 5 : Saisonnalité par mois et sexe
// ─────────────────────────────────────────────
async function requete5() {
  const resultats = await db.raw(`
    SELECT
      s.libelle                                                 AS sexe,
      dt.mois,
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
  afficherResultats('REQUÊTE 5 — Saisonnalité des décès par mois et sexe', resultats.rows);
  sauvegarderJson('req5_saisonnalite.json', resultats.rows);
  return resultats.rows;
}

// ─────────────────────────────────────────────
// REQUÊTE 6 : Gravité par rôle et sexe
// ─────────────────────────────────────────────
async function requete6() {
  const resultats = await db.raw(`
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
        NULLIF(COUNT(*), 0), 2
      )                                                         AS taux_mortalite_pct
    FROM fait_usagers fu
    JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
    WHERE fu.categorie_usager IN (1, 2, 3)
    GROUP BY fu.id_sexe, s.libelle, fu.categorie_usager
    ORDER BY fu.id_sexe, taux_mortalite_pct DESC
  `);
  afficherResultats('REQUÊTE 6 — Mortalité par rôle (conducteur/passager/piéton) et sexe', resultats.rows);
  sauvegarderJson('req6_role_sexe.json', resultats.rows);
  return resultats.rows;
}

// ─────────────────────────────────────────────
// REQUÊTE 7 : Top véhicules par sexe
// ─────────────────────────────────────────────
async function requete7() {
  const resultats = await db.raw(`
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
      JOIN dim_sexe               s  ON fu.id_sexe       = s.id_sexe
      JOIN dim_vehicule           dv ON fu.id_vehicule   = dv.id_vehicule
      JOIN dim_categorie_vehicule cv ON dv.id_categorie  = cv.id_categorie
      GROUP BY fu.id_sexe, s.libelle, dv.id_categorie, cv.libelle
    )
    SELECT sexe, rang, type_vehicule, total, nb_tues
    FROM stats
    WHERE rang <= 5
    ORDER BY sexe, rang
  `);
  afficherResultats('REQUÊTE 7 — Top 5 types de véhicules par sexe', resultats.rows);
  sauvegarderJson('req7_vehicules.json', resultats.rows);
  return resultats.rows;
}

// ─────────────────────────────────────────────
// REQUÊTE 8 : Vue récapitulative complète
// ─────────────────────────────────────────────
async function requete8() {
  const resultats = await db.raw(`
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
      SELECT SUM(total_impliques) AS grand_total, SUM(nb_tues) AS total_tues
      FROM base
    )
    SELECT
      b.sexe,
      b.total_impliques,
      ROUND(b.total_impliques * 100.0 / t.grand_total, 1)     AS pct_du_total,
      b.nb_tues,
      ROUND(b.nb_tues * 100.0 / NULLIF(b.total_impliques, 0), 2)
                                                                AS taux_mortalite_pct,
      ROUND(b.nb_tues * 100.0 / NULLIF(t.total_tues, 0), 1)   AS part_tues_pct,
      b.nb_hospitalises,
      b.nb_blesses_legers,
      b.age_moyen,
      b.nb_accidents_distincts
    FROM base b
    CROSS JOIN totaux t
    ORDER BY b.id_sexe
  `);
  afficherResultats('REQUÊTE 8 — Vue récapitulative complète (CTE + Window Functions)', resultats.rows);
  sauvegarderJson('req8_recap_complet.json', resultats.rows);
  return resultats.rows;
}

// ─────────────────────────────────────────────
// REQUÊTE 9 : Évolution du taux de mortalité par année et sexe
// ─────────────────────────────────────────────
async function requete9() {
  const resultats = await db.raw(`
    SELECT
      s.libelle                                                 AS sexe,
      fu.annee_donnees                                          AS annee,
      COUNT(*)                                                  AS total_victimes,
      COUNT(*) FILTER (WHERE fu.id_gravite = 2)                AS nb_tues,
      ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        NULLIF(COUNT(*), 0), 2
      )                                                         AS taux_mortalite_pct,
      ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        NULLIF(SUM(COUNT(*) FILTER (WHERE fu.id_gravite = 2))
               OVER (PARTITION BY fu.annee_donnees), 0), 1
      )                                                         AS part_tues_annee_pct
    FROM fait_usagers fu
    JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
    WHERE fu.annee_donnees IS NOT NULL
    GROUP BY fu.id_sexe, s.libelle, fu.annee_donnees
    ORDER BY fu.annee_donnees, fu.id_sexe
  `);
  afficherResultats('REQUÊTE 9 — Évolution du taux de mortalité par année et sexe', resultats.rows);
  sauvegarderJson('req9_evolution_annee.json', resultats.rows);
  return resultats.rows;
}

// ─────────────────────────────────────────────
// REQUÊTE 10 : Nombre total de tués par année et sexe
// ─────────────────────────────────────────────
async function requete10() {
  const resultats = await db.raw(`
    SELECT
      fu.annee_donnees                                          AS annee,
      COUNT(*) FILTER (WHERE fu.id_sexe = 1 AND fu.id_gravite = 2) AS tues_hommes,
      COUNT(*) FILTER (WHERE fu.id_sexe = 2 AND fu.id_gravite = 2) AS tues_femmes,
      COUNT(*) FILTER (WHERE fu.id_gravite = 2)                AS total_tues,
      ROUND(
        COUNT(*) FILTER (WHERE fu.id_sexe = 1 AND fu.id_gravite = 2) * 100.0 /
        NULLIF(COUNT(*) FILTER (WHERE fu.id_sexe = 1), 0), 2
      )                                                         AS taux_mortalite_hommes,
      ROUND(
        COUNT(*) FILTER (WHERE fu.id_sexe = 2 AND fu.id_gravite = 2) * 100.0 /
        NULLIF(COUNT(*) FILTER (WHERE fu.id_sexe = 2), 0), 2
      )                                                         AS taux_mortalite_femmes
    FROM fait_usagers fu
    WHERE fu.annee_donnees IS NOT NULL
    GROUP BY fu.annee_donnees
    ORDER BY fu.annee_donnees
  `);
  afficherResultats('REQUÊTE 10 — Nombre de tués par année et sexe', resultats.rows);
  sauvegarderJson('req10_tues_par_annee.json', resultats.rows);
  return resultats.rows;
}

// ─────────────────────────────────────────────
// REQUÊTE 11 : Année la plus meurtrière par sexe
// ─────────────────────────────────────────────
async function requete11() {
  const resultats = await db.raw(`
    WITH tues_par_annee AS (
      SELECT
        s.libelle                                               AS sexe,
        fu.annee_donnees                                        AS annee,
        COUNT(*) FILTER (WHERE fu.id_gravite = 2)              AS nb_tues,
        RANK() OVER (
          PARTITION BY fu.id_sexe
          ORDER BY COUNT(*) FILTER (WHERE fu.id_gravite = 2) DESC
        )                                                       AS rang
      FROM fait_usagers fu
      JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
      WHERE fu.annee_donnees IS NOT NULL
      GROUP BY fu.id_sexe, s.libelle, fu.annee_donnees
    )
    SELECT sexe, annee, nb_tues, rang
    FROM tues_par_annee
    WHERE rang <= 3
    ORDER BY sexe, rang
  `);
  afficherResultats('REQUÊTE 11 — Top 3 années les plus meurtrières par sexe', resultats.rows);
  sauvegarderJson('req11_annees_meurtieres.json', resultats.rows);
  return resultats.rows;
}

// ─────────────────────────────────────────────
// REQUÊTE 12 : Évolution du ratio H/F parmi les tués
// ─────────────────────────────────────────────
async function requete12() {
  const resultats = await db.raw(`
    WITH pivot AS (
      SELECT
        fu.annee_donnees                                        AS annee,
        COUNT(*) FILTER (WHERE fu.id_sexe = 1 AND fu.id_gravite = 2) AS tues_h,
        COUNT(*) FILTER (WHERE fu.id_sexe = 2 AND fu.id_gravite = 2) AS tues_f,
        COUNT(*) FILTER (WHERE fu.id_gravite = 2)              AS total_tues
      FROM fait_usagers fu
      WHERE fu.annee_donnees IS NOT NULL
      GROUP BY fu.annee_donnees
    )
    SELECT
      annee,
      tues_h,
      tues_f,
      total_tues,
      ROUND(tues_h * 100.0 / NULLIF(total_tues, 0), 1)         AS pct_hommes,
      ROUND(tues_f * 100.0 / NULLIF(total_tues, 0), 1)         AS pct_femmes,
      ROUND(tues_h::NUMERIC / NULLIF(tues_f, 0), 2)            AS ratio_h_pour_1f,
      ROUND(
        tues_h * 100.0 / NULLIF(total_tues, 0) -
        LAG(tues_h * 100.0 / NULLIF(total_tues, 0)) OVER (ORDER BY annee),
        1
      )                                                         AS variation_pct_vs_annee_prec
    FROM pivot
    ORDER BY annee
  `);
  afficherResultats('REQUÊTE 12 — Évolution du ratio H/F parmi les tués', resultats.rows);
  sauvegarderJson('req12_ratio_hf_annees.json', resultats.rows);
  return resultats.rows;
}

// ─────────────────────────────────────────────
// REQUÊTE 13 : Avant / Pendant / Après COVID
// ─────────────────────────────────────────────
async function requete13() {
  const resultats = await db.raw(`
    WITH periodes AS (
      SELECT
        fu.id_sexe,
        s.libelle                                               AS sexe,
        CASE
          WHEN fu.annee_donnees = 2019              THEN 'Avant COVID (2019)'
          WHEN fu.annee_donnees IN (2020, 2021)     THEN 'Pendant COVID (2020-2021)'
          WHEN fu.annee_donnees = 2022              THEN 'Après COVID (2022)'
          ELSE 'Autre'
        END                                                     AS periode,
        fu.id_gravite,
        fu.annee_donnees
      FROM fait_usagers fu
      JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
      WHERE fu.annee_donnees IN (2019, 2020, 2021, 2022)
    )
    SELECT
      sexe,
      periode,
      COUNT(*)                                                  AS total_victimes,
      COUNT(*) FILTER (WHERE id_gravite = 2)                   AS nb_tues,
      ROUND(
        COUNT(*) FILTER (WHERE id_gravite = 2) * 100.0 /
        NULLIF(COUNT(*), 0), 2
      )                                                         AS taux_mortalite_pct
    FROM periodes
    GROUP BY id_sexe, sexe, periode
    ORDER BY id_sexe,
      CASE periode
        WHEN 'Avant COVID (2019)'        THEN 1
        WHEN 'Pendant COVID (2020-2021)' THEN 2
        WHEN 'Après COVID (2022)'        THEN 3
        ELSE 4
      END
  `);
  afficherResultats('REQUÊTE 13 — Comparaison Avant / Pendant / Après COVID', resultats.rows);
  sauvegarderJson('req13_covid.json', resultats.rows);
  return resultats.rows;
}

// ─────────────────────────────────────────────
// REQUÊTE 14 : Comparaison par région
// ─────────────────────────────────────────────
async function requete14() {
  const resultats = await db.raw(`
    WITH region_mapping AS (
      SELECT
        fu.id_usager,
        fu.id_sexe,
        fu.id_gravite,
        CASE dl.departement
          WHEN '01' THEN 'Auvergne-Rhône-Alpes'
          WHEN '03' THEN 'Auvergne-Rhône-Alpes'
          WHEN '07' THEN 'Auvergne-Rhône-Alpes'
          WHEN '15' THEN 'Auvergne-Rhône-Alpes'
          WHEN '26' THEN 'Auvergne-Rhône-Alpes'
          WHEN '38' THEN 'Auvergne-Rhône-Alpes'
          WHEN '42' THEN 'Auvergne-Rhône-Alpes'
          WHEN '43' THEN 'Auvergne-Rhône-Alpes'
          WHEN '63' THEN 'Auvergne-Rhône-Alpes'
          WHEN '69' THEN 'Auvergne-Rhône-Alpes'
          WHEN '73' THEN 'Auvergne-Rhône-Alpes'
          WHEN '74' THEN 'Auvergne-Rhône-Alpes'
          WHEN '21' THEN 'Bourgogne-Franche-Comté'
          WHEN '25' THEN 'Bourgogne-Franche-Comté'
          WHEN '39' THEN 'Bourgogne-Franche-Comté'
          WHEN '58' THEN 'Bourgogne-Franche-Comté'
          WHEN '70' THEN 'Bourgogne-Franche-Comté'
          WHEN '71' THEN 'Bourgogne-Franche-Comté'
          WHEN '89' THEN 'Bourgogne-Franche-Comté'
          WHEN '90' THEN 'Bourgogne-Franche-Comté'
          WHEN '22' THEN 'Bretagne'
          WHEN '29' THEN 'Bretagne'
          WHEN '35' THEN 'Bretagne'
          WHEN '56' THEN 'Bretagne'
          WHEN '18' THEN 'Centre-Val de Loire'
          WHEN '28' THEN 'Centre-Val de Loire'
          WHEN '36' THEN 'Centre-Val de Loire'
          WHEN '37' THEN 'Centre-Val de Loire'
          WHEN '41' THEN 'Centre-Val de Loire'
          WHEN '45' THEN 'Centre-Val de Loire'
          WHEN '2A' THEN 'Corse'
          WHEN '2B' THEN 'Corse'
          WHEN '08' THEN 'Grand Est'
          WHEN '10' THEN 'Grand Est'
          WHEN '51' THEN 'Grand Est'
          WHEN '52' THEN 'Grand Est'
          WHEN '54' THEN 'Grand Est'
          WHEN '55' THEN 'Grand Est'
          WHEN '57' THEN 'Grand Est'
          WHEN '67' THEN 'Grand Est'
          WHEN '68' THEN 'Grand Est'
          WHEN '88' THEN 'Grand Est'
          WHEN '02' THEN 'Hauts-de-France'
          WHEN '59' THEN 'Hauts-de-France'
          WHEN '60' THEN 'Hauts-de-France'
          WHEN '62' THEN 'Hauts-de-France'
          WHEN '80' THEN 'Hauts-de-France'
          WHEN '75' THEN 'Île-de-France'
          WHEN '77' THEN 'Île-de-France'
          WHEN '78' THEN 'Île-de-France'
          WHEN '91' THEN 'Île-de-France'
          WHEN '92' THEN 'Île-de-France'
          WHEN '93' THEN 'Île-de-France'
          WHEN '94' THEN 'Île-de-France'
          WHEN '95' THEN 'Île-de-France'
          WHEN '14' THEN 'Normandie'
          WHEN '27' THEN 'Normandie'
          WHEN '50' THEN 'Normandie'
          WHEN '61' THEN 'Normandie'
          WHEN '76' THEN 'Normandie'
          WHEN '16' THEN 'Nouvelle-Aquitaine'
          WHEN '17' THEN 'Nouvelle-Aquitaine'
          WHEN '19' THEN 'Nouvelle-Aquitaine'
          WHEN '23' THEN 'Nouvelle-Aquitaine'
          WHEN '24' THEN 'Nouvelle-Aquitaine'
          WHEN '33' THEN 'Nouvelle-Aquitaine'
          WHEN '40' THEN 'Nouvelle-Aquitaine'
          WHEN '47' THEN 'Nouvelle-Aquitaine'
          WHEN '64' THEN 'Nouvelle-Aquitaine'
          WHEN '79' THEN 'Nouvelle-Aquitaine'
          WHEN '86' THEN 'Nouvelle-Aquitaine'
          WHEN '87' THEN 'Nouvelle-Aquitaine'
          WHEN '09' THEN 'Occitanie'
          WHEN '11' THEN 'Occitanie'
          WHEN '12' THEN 'Occitanie'
          WHEN '30' THEN 'Occitanie'
          WHEN '31' THEN 'Occitanie'
          WHEN '32' THEN 'Occitanie'
          WHEN '34' THEN 'Occitanie'
          WHEN '46' THEN 'Occitanie'
          WHEN '48' THEN 'Occitanie'
          WHEN '65' THEN 'Occitanie'
          WHEN '66' THEN 'Occitanie'
          WHEN '81' THEN 'Occitanie'
          WHEN '82' THEN 'Occitanie'
          WHEN '44' THEN 'Pays de la Loire'
          WHEN '49' THEN 'Pays de la Loire'
          WHEN '53' THEN 'Pays de la Loire'
          WHEN '72' THEN 'Pays de la Loire'
          WHEN '85' THEN 'Pays de la Loire'
          WHEN '04' THEN 'Provence-Alpes-Côte d''Azur'
          WHEN '05' THEN 'Provence-Alpes-Côte d''Azur'
          WHEN '06' THEN 'Provence-Alpes-Côte d''Azur'
          WHEN '13' THEN 'Provence-Alpes-Côte d''Azur'
          WHEN '83' THEN 'Provence-Alpes-Côte d''Azur'
          WHEN '84' THEN 'Provence-Alpes-Côte d''Azur'
          WHEN '971' THEN 'Outre-Mer'
          WHEN '972' THEN 'Outre-Mer'
          WHEN '973' THEN 'Outre-Mer'
          WHEN '974' THEN 'Outre-Mer'
          WHEN '976' THEN 'Outre-Mer'
          ELSE 'Non renseigné'
        END AS region
      FROM fait_usagers fu
      JOIN dim_accident da ON fu.num_acc = da.num_acc
      JOIN dim_lieu     dl ON da.id_lieu = dl.id_lieu
    )
    SELECT
      region,
      COUNT(*)                                                          AS total_victimes,
      COUNT(*) FILTER (WHERE id_sexe = 1)                              AS victimes_hommes,
      COUNT(*) FILTER (WHERE id_sexe = 2)                              AS victimes_femmes,
      COUNT(*) FILTER (WHERE id_gravite = 2)                           AS nb_tues,
      COUNT(*) FILTER (WHERE id_gravite = 2 AND id_sexe = 1)          AS tues_hommes,
      COUNT(*) FILTER (WHERE id_gravite = 2 AND id_sexe = 2)          AS tues_femmes,
      ROUND(
        COUNT(*) FILTER (WHERE id_gravite = 2) * 100.0 /
        NULLIF(COUNT(*), 0), 2
      )                                                                 AS taux_mortalite_pct,
      ROUND(
        COUNT(*) FILTER (WHERE id_gravite = 2 AND id_sexe = 1) * 100.0 /
        NULLIF(COUNT(*) FILTER (WHERE id_sexe = 1), 0), 2
      )                                                                 AS taux_mortalite_hommes_pct,
      ROUND(
        COUNT(*) FILTER (WHERE id_gravite = 2 AND id_sexe = 2) * 100.0 /
        NULLIF(COUNT(*) FILTER (WHERE id_sexe = 2), 0), 2
      )                                                                 AS taux_mortalite_femmes_pct
    FROM region_mapping
    GROUP BY region
    ORDER BY nb_tues DESC
  `);
  afficherRegions(resultats.rows);
  sauvegarderJson('req14_regions.json', resultats.rows);
  return resultats.rows;
}

// ─────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────
async function main() {
  console.log('\n' + '█'.repeat(70));
  console.log('  ANALYSE — Accidents de la Route France — Mortalité par sexe');
  console.log('█'.repeat(70));

  try {
    // Test de connexion
    await db.raw('SELECT 1');
    console.log('  ✓ Connexion à la base de données OK');

    // Vérifier les années disponibles en base
    const annees = await db.raw(`
      SELECT annee_donnees, COUNT(*) AS nb_usagers
      FROM fait_usagers
      WHERE annee_donnees IS NOT NULL
      GROUP BY annee_donnees
      ORDER BY annee_donnees
    `);
    console.log(`\n  Années en base : ${annees.rows.map(r => r.annee_donnees).join(', ')}`);
    console.log(`  (${annees.rows.length} année(s) chargée(s))`);

    const multiAnnees = annees.rows.length > 1;

    // Requêtes principales (toujours disponibles)
    await requete1();
    await requete2();
    await requete3();
    await requete4();
    await requete5();
    await requete6();
    await requete7();
    await requete8();
    await requete14();

    // Requêtes multi-années (seulement si plusieurs années en base)
    if (multiAnnees) {
      console.log('\n' + '▓'.repeat(70));
      console.log('  ANALYSES MULTI-ANNÉES (tendances 10 ans)');
      console.log('▓'.repeat(70));
      await requete9();
      await requete10();
      await requete11();
      await requete12();
      await requete13();
    } else {
      console.log('\n  ℹ  Requêtes multi-années ignorées (1 seule année en base)');
      console.log('  → Pour les tendances 10 ans : npm run etl:full');
    }

    console.log('\n' + '═'.repeat(70));
    console.log('  ✓ Toutes les analyses terminées. Résultats dans output/');
    console.log('═'.repeat(70) + '\n');

  } catch (err) {
    console.error('\n  ✗ ERREUR :', err.message);
    if (err.message.includes('relation') && err.message.includes('does not exist')) {
      console.error('  → La base de données est vide. Lancez d\'abord : npm run etl');
    } else if (err.message.includes('ECONNREFUSED') || err.message.includes('password')) {
      console.error('  → Vérifiez votre DATABASE_URL dans le fichier .env');
    }
    process.exit(1);
  } finally {
    await db.destroy();
  }
}

main();
