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
// MAIN
// ─────────────────────────────────────────────
async function main() {
  console.log('\n' + '█'.repeat(70));
  console.log('  ANALYSE — Accidents de la Route France 2022 — Mortalité par sexe');
  console.log('█'.repeat(70));

  try {
    // Test de connexion
    await db.raw('SELECT 1');
    console.log('  ✓ Connexion à la base de données OK');

    await requete1();
    await requete2();
    await requete3();
    await requete4();
    await requete5();
    await requete6();
    await requete7();
    await requete8();

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
