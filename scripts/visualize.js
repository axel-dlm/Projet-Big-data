/**
 * Script de visualisation — génère les données pour le dashboard
 * Connecte à PostgreSQL, exécute les requêtes principales,
 * et met à jour les données dans dashboard.html
 *
 * Utilisation : node scripts/visualize.js
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const knex = require('knex');

const DASHBOARD_PATH = path.join(__dirname, 'dashboard.html');
const OUTPUT_DIR = path.join(__dirname, '..', 'output');

const db = knex({
  client: 'pg',
  connection: {
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  }
});

if (!fs.existsSync(OUTPUT_DIR)) {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

async function fetchDonneesVisualisations() {
  console.log('\n' + '═'.repeat(60));
  console.log('GÉNÉRATION DES DONNÉES DE VISUALISATION');
  console.log('═'.repeat(60));

  // 1. Mortalité par sexe
  console.log('\n  Requête : mortalité par sexe...');
  const mortalite = await db.raw(`
    WITH stats AS (
      SELECT
        fu.id_sexe,
        s.libelle AS sexe,
        COUNT(*) AS total_victimes,
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) AS nb_tues,
        COUNT(*) FILTER (WHERE fu.id_gravite = 3) AS nb_hospitalises,
        COUNT(*) FILTER (WHERE fu.id_gravite = 4) AS nb_blesses_legers,
        COUNT(*) FILTER (WHERE fu.id_gravite = 1) AS nb_indemnes
      FROM fait_usagers fu
      JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
      GROUP BY fu.id_sexe, s.libelle
    )
    SELECT *,
      ROUND(nb_tues * 100.0 / NULLIF(total_victimes, 0), 2) AS taux_mortalite_pct,
      ROUND(nb_tues * 100.0 / NULLIF(SUM(nb_tues) OVER (), 0), 1) AS part_des_tues_pct
    FROM stats ORDER BY id_sexe
  `);
  console.log('  ✓ OK');

  // 2. Décès par mois et sexe
  console.log('  Requête : saisonnalité...');
  const saisonnalite = await db.raw(`
    SELECT
      s.libelle AS sexe,
      dt.mois,
      COUNT(*) FILTER (WHERE fu.id_gravite = 2) AS nb_tues
    FROM fait_usagers fu
    JOIN dim_sexe     s  ON fu.id_sexe  = s.id_sexe
    JOIN dim_accident da ON fu.num_acc  = da.num_acc
    JOIN dim_temps    dt ON da.id_temps = dt.id_temps
    WHERE dt.mois IS NOT NULL
    GROUP BY fu.id_sexe, s.libelle, dt.mois
    ORDER BY fu.id_sexe, dt.mois
  `);
  console.log('  ✓ OK');

  // 3. Décès par tranche d'âge
  console.log('  Requête : tranches d\'âge...');
  const ages = await db.raw(`
    SELECT
      s.libelle AS sexe,
      CASE
        WHEN fu.age_au_moment < 18              THEN 'Moins de 18 ans'
        WHEN fu.age_au_moment BETWEEN 18 AND 25 THEN '18-25 ans'
        WHEN fu.age_au_moment BETWEEN 26 AND 35 THEN '26-35 ans'
        WHEN fu.age_au_moment BETWEEN 36 AND 50 THEN '36-50 ans'
        WHEN fu.age_au_moment BETWEEN 51 AND 65 THEN '51-65 ans'
        WHEN fu.age_au_moment > 65              THEN 'Plus de 65 ans'
      END AS tranche_age,
      COUNT(*) FILTER (WHERE fu.id_gravite = 2) AS nb_tues
    FROM fait_usagers fu
    JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
    WHERE fu.age_au_moment IS NOT NULL
    GROUP BY fu.id_sexe, s.libelle, tranche_age
    ORDER BY fu.id_sexe, MIN(fu.age_au_moment)
  `);
  console.log('  ✓ OK');

  // 4. Mortalité par rôle
  console.log('  Requête : rôle (conducteur/passager/piéton)...');
  const roles = await db.raw(`
    SELECT
      s.libelle AS sexe,
      CASE fu.categorie_usager
        WHEN 1 THEN 'Conducteur'
        WHEN 2 THEN 'Passager'
        WHEN 3 THEN 'Piéton'
      END AS role,
      ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        NULLIF(COUNT(*), 0), 2
      ) AS taux_mortalite_pct
    FROM fait_usagers fu
    JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
    WHERE fu.categorie_usager IN (1, 2, 3)
    GROUP BY fu.id_sexe, s.libelle, fu.categorie_usager
    ORDER BY fu.id_sexe, fu.categorie_usager
  `);
  console.log('  ✓ OK');

  return {
    mortalite: mortalite.rows,
    saisonnalite: saisonnalite.rows,
    ages: ages.rows,
    roles: roles.rows
  };
}

/**
 * Formate les données pour le dashboard et les injecte dans le HTML
 */
function genererDashboard(donnees) {
  const { mortalite, saisonnalite, ages, roles } = donnees;

  // Formater les données de mortalité
  const dataMortalite = mortalite.map(d => ({
    sexe: d.sexe,
    totalVictimes: parseInt(d.total_victimes),
    nbTues: parseInt(d.nb_tues),
    nbHospitalises: parseInt(d.nb_hospitalises),
    nbBlessesLegers: parseInt(d.nb_blesses_legers),
    nbIndemnes: parseInt(d.nb_indemnes),
    tauxMortalitePct: parseFloat(d.taux_mortalite_pct),
    partDesTuesPct: parseFloat(d.part_des_tues_pct)
  }));

  // Formater les données de saisonnalité
  const hommesMois = Array(12).fill(0);
  const femmesMois = Array(12).fill(0);
  saisonnalite.forEach(d => {
    const idx = parseInt(d.mois) - 1;
    if (d.sexe === 'Masculin') hommesMois[idx] = parseInt(d.nb_tues) || 0;
    else femmesMois[idx] = parseInt(d.nb_tues) || 0;
  });

  // Formater les données d'âge
  const labelsAge = ['Moins de 18 ans', '18-25 ans', '26-35 ans', '36-50 ans', '51-65 ans', 'Plus de 65 ans'];
  const hommesAge = labelsAge.map(l => {
    const r = ages.find(d => d.sexe === 'Masculin' && d.tranche_age === l);
    return r ? parseInt(r.nb_tues) : 0;
  });
  const femmesAge = labelsAge.map(l => {
    const r = ages.find(d => d.sexe === 'Féminin' && d.tranche_age === l);
    return r ? parseInt(r.nb_tues) : 0;
  });

  // Formater les données de rôles
  const labelsRole = ['Conducteur', 'Passager', 'Piéton'];
  const tauxHommesRole = labelsRole.map(l => {
    const r = roles.find(d => d.sexe === 'Masculin' && d.role === l);
    return r ? parseFloat(r.taux_mortalite_pct) : 0;
  });
  const tauxFemmesRole = labelsRole.map(l => {
    const r = roles.find(d => d.sexe === 'Féminin' && d.role === l);
    return r ? parseFloat(r.taux_mortalite_pct) : 0;
  });

  // Bloc JS à injecter dans le dashboard
  const blockJS = `
// ================================================================
// DONNÉES ISSUES DES REQUÊTES SQL (à mettre à jour après exécution)
// Ces valeurs correspondent aux résultats typiques de l'année 2022
// ================================================================

// Requête 2 : taux de mortalité par sexe
const DATA_MORTALITE = ${JSON.stringify(dataMortalite, null, 2)};

// Requête 3 : décès par tranche d'âge
const DATA_AGE = {
  labels: ${JSON.stringify(labelsAge)},
  hommes: ${JSON.stringify(hommesAge)},
  femmes: ${JSON.stringify(femmesAge)}
};

// Requête 5 : décès par mois
const DATA_MOIS = {
  labels: ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Jun', 'Jul', 'Aoû', 'Sep', 'Oct', 'Nov', 'Déc'],
  hommes: ${JSON.stringify(hommesMois)},
  femmes: ${JSON.stringify(femmesMois)}
};

// Requête 6 : rôle (conducteur, passager, piéton)
const DATA_ROLE = {
  labels: ${JSON.stringify(labelsRole)},
  tauxHommes: ${JSON.stringify(tauxHommesRole)},
  tauxFemmes: ${JSON.stringify(tauxFemmesRole)}
};`;

  // Lire le dashboard HTML existant et remplacer le bloc de données
  let html = fs.readFileSync(DASHBOARD_PATH, 'utf-8');

  // Remplacer le bloc entre les marqueurs de données
  const regex = /\/\/ ={60,}\n\/\/ DONNÉES ISSUES DES REQUÊTES SQL[\s\S]*?\/\/ Requête 6 : rôle \(conducteur, passager, piéton\)\n[\s\S]*?\};/;

  if (regex.test(html)) {
    html = html.replace(regex, blockJS.trim());
    fs.writeFileSync(DASHBOARD_PATH, html, 'utf-8');
    console.log('\n  ✓ Dashboard mis à jour avec les données réelles : scripts/dashboard.html');
  } else {
    // Sauvegarder les données dans un fichier JSON séparé
    const cheminJson = path.join(OUTPUT_DIR, 'dashboard_data.json');
    fs.writeFileSync(cheminJson, JSON.stringify({
      mortalite: dataMortalite,
      mois: { hommes: hommesMois, femmes: femmesMois },
      ages: { labels: labelsAge, hommes: hommesAge, femmes: femmesAge },
      roles: { labels: labelsRole, tauxHommes: tauxHommesRole, tauxFemmes: tauxFemmesRole }
    }, null, 2), 'utf-8');
    console.log('\n  ✓ Données exportées : output/dashboard_data.json');
    console.log('  → Copiez ces données dans le dashboard.html manuellement');
  }
}

async function main() {
  console.log('\n' + '█'.repeat(60));
  console.log('  VISUALISATION — Génération du Dashboard');
  console.log('█'.repeat(60));

  try {
    await db.raw('SELECT 1');
    console.log('  ✓ Connexion à la base de données OK');

    const donnees = await fetchDonneesVisualisations();
    genererDashboard(donnees);

    console.log('\n  → Ouvrez scripts/dashboard.html dans votre navigateur');
    console.log('    (double-clic ou Live Server dans VS Code)\n');

  } catch (err) {
    console.error('\n  ✗ ERREUR :', err.message);
    if (err.message.includes('relation') && err.message.includes('does not exist')) {
      console.error('  → Base vide. Lancez d\'abord : npm run etl');
    }
    process.exit(1);
  } finally {
    await db.destroy();
  }
}

main();
