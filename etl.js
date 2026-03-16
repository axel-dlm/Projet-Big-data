/**
 * Pipeline ETL principal - Accidents de la route en France 2022
 * Thématique : Comparaison de la mortalité par sexe
 *
 * Phases :
 *   1. EXTRACT  → Lecture des fichiers CSV (caractéristiques, usagers, véhicules)
 *   2. TRANSFORM → Nettoyage, conversion, enrichissement des données
 *   3. LOAD     → Insertion en base PostgreSQL (Neon) via Knex.js
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const knex = require('knex');

// ─────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────

const DATA_DIR = path.join(__dirname, 'data');
const SQL_DIR = path.join(__dirname, 'sql');
const BATCH_SIZE = 100; // Nombre de lignes insérées par batch

// Valeur "inconnue" dans les datasets de l'ONISR
const VALEUR_INCONNUE = -1;

// Connexion Knex vers PostgreSQL (Neon)
const db = knex({
  client: 'pg',
  connection: {
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  },
  pool: { min: 1, max: 5 }
});

// ─────────────────────────────────────────────
// UTILITAIRES
// ─────────────────────────────────────────────

/** Convertit une valeur en entier, retourne null si invalide ou inconnue */
function toInt(val) {
  if (val === null || val === undefined || val === '' || val === '-1') return null;
  const n = parseInt(val, 10);
  return isNaN(n) ? null : n;
}

/** Convertit une valeur en float, retourne null si invalide */
function toFloat(val) {
  if (val === null || val === undefined || val === '') return null;
  const f = parseFloat(String(val).replace(',', '.'));
  return isNaN(f) ? null : f;
}

/** Nettoie une chaîne de caractères */
function toStr(val) {
  if (val === null || val === undefined) return null;
  const s = String(val).trim();
  return s === '' || s === '-1' ? null : s;
}

/** Calcule l'âge de l'usager au moment de l'accident */
function calculerAge(anneeAccident, anneeNaissance) {
  const a = toInt(anneeAccident);
  const n = toInt(anneeNaissance);
  if (!a || !n || n < 1900 || n > a) return null;
  const age = a - n;
  return age > 0 && age < 120 ? age : null;
}

/** Calcule la tranche horaire à partir de l'heure */
function trancheHoraire(heure) {
  const h = toInt(heure);
  if (h === null) return null;
  if (h >= 6 && h < 12) return 'matin';
  if (h >= 12 && h < 18) return 'apres-midi';
  if (h >= 18 && h < 22) return 'soiree';
  return 'nuit';
}

/** Calcule le trimestre à partir du mois */
function calculerTrimestre(mois) {
  const m = toInt(mois);
  if (!m) return null;
  return Math.ceil(m / 3);
}

/** Insère des données par batch pour éviter les timeouts */
async function insererParBatch(table, donnees, label) {
  let inseres = 0;
  for (let i = 0; i < donnees.length; i += BATCH_SIZE) {
    const batch = donnees.slice(i, i + BATCH_SIZE);
    await db(table).insert(batch).onConflict().ignore();
    inseres += batch.length;
    process.stdout.write(`\r  ${label} : ${inseres}/${donnees.length} lignes insérées`);
  }
  console.log(); // Nouvelle ligne après la progression
  return inseres;
}

// ─────────────────────────────────────────────
// PHASE 1 : EXTRACT
// ─────────────────────────────────────────────

async function extract() {
  console.log('\n' + '═'.repeat(60));
  console.log('PHASE 1 — EXTRACT');
  console.log('═'.repeat(60));

  const lireCsv = (nomFichier) => {
    const chemin = path.join(DATA_DIR, nomFichier);
    if (!fs.existsSync(chemin)) {
      throw new Error(`Fichier introuvable : ${chemin}\nLancez d'abord : npm run download`);
    }
    const contenu = fs.readFileSync(chemin, 'utf-8');
    // Supprime le BOM UTF-8 si présent
    const contenuPropre = contenu.replace(/^\uFEFF/, '');
    const lignes = parse(contenuPropre, {
      delimiter: ';',
      columns: true,          // Utilise la première ligne comme en-têtes
      skip_empty_lines: true,
      trim: true,
      relax_column_count: true
    });
    return lignes;
  };

  console.log('Lecture des fichiers CSV...');
  const caracteristiques = lireCsv('caracteristiques-2022.csv');
  console.log(`  ✓ caracteristiques-2022.csv : ${caracteristiques.length} lignes`);

  const usagers = lireCsv('usagers-2022.csv');
  console.log(`  ✓ usagers-2022.csv          : ${usagers.length} lignes`);

  const vehicules = lireCsv('vehicules-2022.csv');
  console.log(`  ✓ vehicules-2022.csv        : ${vehicules.length} lignes`);

  return { caracteristiques, usagers, vehicules };
}

// ─────────────────────────────────────────────
// PHASE 2 : TRANSFORM
// ─────────────────────────────────────────────

async function transform({ caracteristiques, usagers, vehicules }) {
  console.log('\n' + '═'.repeat(60));
  console.log('PHASE 2 — TRANSFORM');
  console.log('═'.repeat(60));

  // --- Transformation des caractéristiques (accidents) ---
  console.log('\nTransformation des accidents...');
  const accidentsTransformes = caracteristiques.map((row) => {
    const heure = toStr(row.hrmn) ? String(row.hrmn).split(':')[0] : null;
    const minute = toStr(row.hrmn) ? String(row.hrmn).split(':')[1] : null;
    const mois = toInt(row.mois);
    return {
      num_acc: toStr(row.Accident_Id || row.num_acc || row.Num_Acc),
      jour: toInt(row.jour),
      mois: mois,
      annee: toInt(row.an) || 2022,
      heure: toInt(heure),
      minute: toInt(minute),
      trimestre: calculerTrimestre(mois),
      tranche_horaire: trancheHoraire(heure),
      departement: toStr(row.dep),
      commune: toStr(row.com),
      agglomeration: toInt(row.agg),
      adresse: toStr(row.adr),
      latitude: toFloat(row.lat),
      longitude: toFloat(row.long),
      luminosite: toInt(row.lum),
      conditions_atmo: toInt(row.atm),
      type_intersection: toInt(row.int),
      type_collision: toInt(row.col)
    };
  }).filter(a => a.num_acc !== null);

  console.log(`  ✓ ${accidentsTransformes.length} accidents valides`);

  // --- Transformation des véhicules ---
  console.log('\nTransformation des véhicules...');
  const vehiculesTransformes = vehicules.map((row) => ({
    num_acc: toStr(row.Accident_Id || row.num_acc || row.Num_Acc),
    num_veh: toStr(row.id_vehicule || row.num_veh || row.Num_Veh),
    categorie_vehicule: toInt(row.catv),
    sens: toInt(row.sens),
    obstacle_fixe: toInt(row.obs),
    obstacle_mobile: toInt(row.obsm),
    point_choc: toInt(row.choc),
    manoeuvre: toInt(row.manv),
    moteur: toInt(row.motor)
  })).filter(v => v.num_acc !== null && v.num_veh !== null);

  console.log(`  ✓ ${vehiculesTransformes.length} véhicules valides`);

  // --- Transformation des usagers ---
  console.log('\nTransformation des usagers...');

  // Compter avant filtrage
  let nbHommes = 0, nbFemmes = 0;
  let nbTuesHommes = 0, nbTuesFemmes = 0;
  let nbIgnorésSexe = 0;
  let nbIgnoresGravite = 0;

  const usagersTransformes = usagers.map((row) => {
    const sexe = toInt(row.sexe);
    const gravite = toInt(row.grav);
    const anneeNaissance = toInt(row.an_nais);
    const anneeAccident = toInt(row.an) || 2022;

    // Validation du sexe : doit être 1 (masculin) ou 2 (féminin)
    if (sexe !== 1 && sexe !== 2) {
      nbIgnorésSexe++;
      return null;
    }

    // Validation de la gravité : doit être entre 1 et 4
    if (gravite === null || gravite < 1 || gravite > 4) {
      nbIgnoresGravite++;
      return null;
    }

    // Compter par sexe
    if (sexe === 1) {
      nbHommes++;
      if (gravite === 2) nbTuesHommes++;
    } else {
      nbFemmes++;
      if (gravite === 2) nbTuesFemmes++;
    }

    const age = calculerAge(anneeAccident, anneeNaissance);

    return {
      num_acc: toStr(row.Accident_Id || row.num_acc || row.Num_Acc),
      num_veh: toStr(row.id_vehicule || row.num_veh || row.Num_Veh),
      place: toInt(row.place),
      categorie_usager: toInt(row.catu),
      gravite: gravite,
      sexe: sexe,
      annee_naissance: anneeNaissance,
      age_au_moment: age,
      type_trajet: toInt(row.trajet),
      equipement_securite: toInt(row.secu1)
    };
  }).filter(u => u !== null);

  console.log(`  ✓ ${usagersTransformes.length} usagers valides`);
  console.log(`  ✗ ${nbIgnorésSexe} usagers ignorés (sexe inconnu)`);
  console.log(`  ✗ ${nbIgnoresGravite} usagers ignorés (gravité inconnue)`);
  console.log('\n  Statistiques par sexe :');
  console.log(`  → Hommes   : ${nbHommes.toLocaleString('fr-FR')} usagers, dont ${nbTuesHommes} tués`);
  console.log(`  → Femmes   : ${nbFemmes.toLocaleString('fr-FR')} usagères, dont ${nbTuesFemmes} tuées`);
  const totalTues = nbTuesHommes + nbTuesFemmes;
  if (totalTues > 0) {
    console.log(`  → Part des hommes parmi les tués : ${Math.round((nbTuesHommes / totalTues) * 100)}%`);
  }

  return { accidentsTransformes, vehiculesTransformes, usagersTransformes };
}

// ─────────────────────────────────────────────
// PHASE 3 : LOAD
// ─────────────────────────────────────────────

async function load({ accidentsTransformes, vehiculesTransformes, usagersTransformes }) {
  console.log('\n' + '═'.repeat(60));
  console.log('PHASE 3 — LOAD');
  console.log('═'.repeat(60));

  // 1. Créer les tables (exécuter le schéma SQL)
  console.log('\nCréation du schéma de base de données...');
  const schemaSql = fs.readFileSync(path.join(SQL_DIR, '01_schema.sql'), 'utf-8');

  // Exécute le schéma bloc par bloc (séparé par les points-virgules)
  // Supprimer les commentaires SQL (-- ...) avant de découper par ";"
  // sans ça, les blocs de commentaires qui précèdent un CREATE TABLE
  // font croire que la déclaration commence par "--" et elle est ignorée
  const sqlNettoye = schemaSql.replace(/--[^\n]*/g, '');
  const statements = sqlNettoye
    .split(';')
    .map(s => s.trim())
    .filter(s => s.length > 0);

  for (const stmt of statements) {
    await db.raw(stmt);
  }
  console.log('  ✓ Tables créées avec succès');

  // 2. Insérer les tables de dimensions statiques
  console.log('\nInsertion des données de référence (dimensions)...');

  await db('dim_sexe').insert([
    { id_sexe: 1, libelle: 'Masculin' },
    { id_sexe: 2, libelle: 'Féminin' }
  ]).onConflict('id_sexe').ignore();
  console.log('  ✓ dim_sexe');

  await db('dim_gravite').insert([
    { id_gravite: 1, libelle: 'Indemne',       niveau_severite: 0 },
    { id_gravite: 2, libelle: 'Tué',            niveau_severite: 3 },
    { id_gravite: 3, libelle: 'Hospitalisé',    niveau_severite: 2 },
    { id_gravite: 4, libelle: 'Blessé léger',   niveau_severite: 1 }
  ]).onConflict('id_gravite').ignore();
  console.log('  ✓ dim_gravite');

  await db('dim_luminosite').insert([
    { id_luminosite: 1, libelle: 'Plein jour' },
    { id_luminosite: 2, libelle: 'Crépuscule ou aube' },
    { id_luminosite: 3, libelle: 'Nuit sans éclairage public' },
    { id_luminosite: 4, libelle: 'Nuit avec éclairage public non allumé' },
    { id_luminosite: 5, libelle: 'Nuit avec éclairage public allumé' }
  ]).onConflict('id_luminosite').ignore();
  console.log('  ✓ dim_luminosite');

  await db('dim_atmosphere').insert([
    { id_atmosphere: 1, libelle: 'Normale' },
    { id_atmosphere: 2, libelle: 'Pluie légère' },
    { id_atmosphere: 3, libelle: 'Pluie forte' },
    { id_atmosphere: 4, libelle: 'Neige ou grêle' },
    { id_atmosphere: 5, libelle: 'Brouillard ou fumée' },
    { id_atmosphere: 6, libelle: 'Vent fort ou tempête' },
    { id_atmosphere: 7, libelle: 'Temps éblouissant' },
    { id_atmosphere: 8, libelle: 'Temps couvert' },
    { id_atmosphere: 9, libelle: 'Autre' }
  ]).onConflict('id_atmosphere').ignore();
  console.log('  ✓ dim_atmosphere');

  await db('dim_categorie_vehicule').insert([
    { id_categorie: 1,  libelle: 'Bicyclette' },
    { id_categorie: 2,  libelle: 'Cyclomoteur <50cm3' },
    { id_categorie: 3,  libelle: 'Voiturette' },
    { id_categorie: 7,  libelle: 'VL seul' },
    { id_categorie: 10, libelle: 'VU seul 1,5T<=PTAC<=3,5T' },
    { id_categorie: 13, libelle: 'PL seul 3,5T<PTCA<=7,5T' },
    { id_categorie: 14, libelle: 'PL seul > 7,5T' },
    { id_categorie: 15, libelle: 'PL > 3,5T + remorque' },
    { id_categorie: 16, libelle: 'Tracteur routier seul' },
    { id_categorie: 17, libelle: 'Tracteur routier + semi-remorque' },
    { id_categorie: 20, libelle: 'Engin spécial' },
    { id_categorie: 21, libelle: 'Tracteur agricole' },
    { id_categorie: 30, libelle: 'Scooter <50cm3' },
    { id_categorie: 31, libelle: 'Motocyclette >50cm3 et <=125cm3' },
    { id_categorie: 32, libelle: 'Scooter >50cm3 et <=125cm3' },
    { id_categorie: 33, libelle: 'Motocyclette >125cm3' },
    { id_categorie: 34, libelle: 'Scooter >125cm3' },
    { id_categorie: 35, libelle: 'Quad léger <=50cm3' },
    { id_categorie: 36, libelle: 'Quad lourd >50cm3' },
    { id_categorie: 37, libelle: 'Autobus' },
    { id_categorie: 38, libelle: 'Autocar' },
    { id_categorie: 39, libelle: 'Train' },
    { id_categorie: 40, libelle: 'Tramway' },
    { id_categorie: 41, libelle: 'Tricycle à moteur <=50cm3' },
    { id_categorie: 42, libelle: 'Tricycle à moteur >50cm3' },
    { id_categorie: 43, libelle: 'EDP à moteur' },
    { id_categorie: 99, libelle: 'Autre' }
  ]).onConflict('id_categorie').ignore();
  console.log('  ✓ dim_categorie_vehicule');

  // 3. Insérer les dimensions liées aux accidents
  console.log('\nInsertion des données de temps et de lieux...');

  // dim_temps : une ligne par accident (dates/heures)
  const tempsDonnees = accidentsTransformes.map((acc, idx) => ({
    id_temps: idx + 1,
    jour: acc.jour,
    mois: acc.mois,
    annee: acc.annee,
    heure: acc.heure,
    minute: acc.minute,
    trimestre: acc.trimestre,
    tranche_horaire: acc.tranche_horaire
  }));
  await insererParBatch('dim_temps', tempsDonnees, 'dim_temps');

  // dim_lieu : une ligne par accident (coordonnées géographiques)
  const lieuxDonnees = accidentsTransformes.map((acc, idx) => ({
    id_lieu: idx + 1,
    departement: acc.departement,
    commune: acc.commune,
    agglomeration: acc.agglomeration,
    adresse: acc.adresse ? acc.adresse.substring(0, 500) : null,
    latitude: acc.latitude,
    longitude: acc.longitude
  }));
  await insererParBatch('dim_lieu', lieuxDonnees, 'dim_lieu');

  // 4. Insérer dim_accident (table de dimension centrale)
  console.log('\nInsertion des accidents...');
  // Valeurs valides dans les tables de dimensions
  const LUMINOSITE_VALIDES = [1, 2, 3, 4, 5];
  const ATMOSPHERE_VALIDES = [1, 2, 3, 4, 5, 6, 7, 8, 9];

  const accidentsDonnees = accidentsTransformes.map((acc, idx) => ({
    num_acc: acc.num_acc,
    id_temps: idx + 1,
    id_lieu: idx + 1,
    // Mettre NULL si la valeur n'existe pas dans la dimension (évite FK violation)
    id_luminosite: LUMINOSITE_VALIDES.includes(acc.luminosite) ? acc.luminosite : null,
    id_atmosphere: ATMOSPHERE_VALIDES.includes(acc.conditions_atmo) ? acc.conditions_atmo : null,
    type_intersection: acc.type_intersection,
    type_collision: acc.type_collision
  }));
  await insererParBatch('dim_accident', accidentsDonnees, 'dim_accident');

  // 5. Créer un mapping num_acc → id_accident pour les clés étrangères
  console.log('\nConstruction des index de référence...');
  const accidentsRef = {};
  accidentsDonnees.forEach((a, idx) => {
    accidentsRef[a.num_acc] = idx + 1;
  });

  // 6. Insérer dim_vehicule
  console.log('\nInsertion des véhicules...');
  const CATEGORIES_VALIDES = [1,2,3,7,10,13,14,15,16,17,20,21,30,31,32,33,34,35,36,37,38,39,40,41,42,43,99];

  const vehiculesDonnees = vehiculesTransformes.map((v, idx) => ({
    id_vehicule: idx + 1,
    num_acc: v.num_acc,
    num_veh: v.num_veh,
    id_categorie: CATEGORIES_VALIDES.includes(v.categorie_vehicule) ? v.categorie_vehicule : null,
    sens: v.sens,
    obstacle: v.obstacle_fixe,
    choc: v.point_choc,
    manoeuvre: v.manoeuvre
  }));
  await insererParBatch('dim_vehicule', vehiculesDonnees, 'dim_vehicule');

  // Index : num_acc + num_veh → id_vehicule
  const vehiculesRef = {};
  vehiculesDonnees.forEach((v) => {
    vehiculesRef[`${v.num_acc}_${v.num_veh}`] = v.id_vehicule;
  });

  // 7. Insérer la table de faits
  console.log('\nInsertion de la table de faits (usagers)...');
  let idUsager = 1;
  const faitsDonnees = usagersTransformes.map((u) => {
    const idVehicule = vehiculesRef[`${u.num_acc}_${u.num_veh}`] || null;
    return {
      id_usager: idUsager++,
      num_acc: u.num_acc,
      id_vehicule: idVehicule,
      id_sexe: u.sexe,
      id_gravite: u.gravite,
      place: u.place,
      categorie_usager: u.categorie_usager,
      annee_naissance: u.annee_naissance,
      age_au_moment: u.age_au_moment,
      type_trajet: u.type_trajet,
      equipement_securite: u.equipement_securite
    };
  });
  await insererParBatch('fait_usagers', faitsDonnees, 'fait_usagers');

  console.log('\n' + '═'.repeat(60));
  console.log('CHARGEMENT TERMINÉ');
  console.log('═'.repeat(60));
  console.log(`  Accidents   : ${accidentsTransformes.length} lignes`);
  console.log(`  Véhicules   : ${vehiculesDonnees.length} lignes`);
  console.log(`  Usagers     : ${faitsDonnees.length} lignes`);
}

// ─────────────────────────────────────────────
// MAIN — Orchestration du pipeline
// ─────────────────────────────────────────────

async function main() {
  console.log('\n' + '█'.repeat(60));
  console.log('  PIPELINE ETL — ACCIDENTS DE LA ROUTE EN FRANCE 2022');
  console.log('  Thématique : Mortalité comparée par sexe');
  console.log('█'.repeat(60));
  console.log(`  Démarrage : ${new Date().toLocaleString('fr-FR')}`);

  const debut = Date.now();

  try {
    // EXTRACT
    const brut = await extract();

    // TRANSFORM
    const transforme = await transform(brut);

    // LOAD
    await load(transforme);

    const duree = Math.round((Date.now() - debut) / 1000);
    console.log(`\n  ✓ Pipeline terminé en ${duree} secondes`);
    console.log('  → Vous pouvez maintenant exécuter : npm run analyze\n');

  } catch (err) {
    console.error('\n  ✗ ERREUR FATALE :', err.message);
    if (err.message.includes('Fichier introuvable')) {
      console.error('  → Lancez d\'abord : npm run download');
    } else if (err.message.includes('ECONNREFUSED') || err.message.includes('password')) {
      console.error('  → Vérifiez votre DATABASE_URL dans le fichier .env');
    }
    process.exit(1);
  } finally {
    await db.destroy();
  }
}

main();
