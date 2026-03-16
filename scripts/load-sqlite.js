/**
 * Chargement des données accidents dans une base SQLite locale
 * Permet de travailler offline sans connexion à Neon/PostgreSQL
 *
 * Usage :
 *   node scripts/load-sqlite.js          → charge l'année 2022 uniquement
 *   node scripts/load-sqlite.js --all    → charge toutes les années disponibles
 *   node scripts/load-sqlite.js 2020     → charge une année spécifique
 *
 * Prérequis : avoir téléchargé les CSV (npm run download)
 */

const fs      = require('fs');
const path    = require('path');
const { parse } = require('csv-parse/sync');
const Database  = require('better-sqlite3');

// ─────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────

const DATA_DIR   = path.join(__dirname, '..', 'data');
const SQL_DIR    = path.join(__dirname, '..', 'sql');
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const DB_PATH    = path.join(OUTPUT_DIR, 'accidents.db');

const LUMINOSITE_VALIDES  = [1, 2, 3, 4, 5];
const ATMOSPHERE_VALIDES  = [1, 2, 3, 4, 5, 6, 7, 8, 9];
const CATEGORIES_VALIDES  = [1,2,3,7,10,13,14,15,16,17,20,21,30,31,32,33,34,35,36,37,38,39,40,41,42,43,99];

// ─────────────────────────────────────────────
// UTILITAIRES (identiques à etl.js)
// ─────────────────────────────────────────────

function toInt(val) {
  if (val === null || val === undefined || val === '' || val === '-1') return null;
  const n = parseInt(val, 10);
  return isNaN(n) ? null : n;
}

function toFloat(val) {
  if (val === null || val === undefined || val === '') return null;
  const f = parseFloat(String(val).replace(',', '.'));
  return isNaN(f) ? null : f;
}

function toStr(val) {
  if (val === null || val === undefined) return null;
  const s = String(val).trim();
  return s === '' || s === '-1' ? null : s;
}

function calculerAge(anneeAccident, anneeNaissance) {
  const a = toInt(anneeAccident);
  const n = toInt(anneeNaissance);
  if (!a || !n || n < 1900 || n > a) return null;
  const age = a - n;
  return age > 0 && age < 120 ? age : null;
}

function trancheHoraire(heure) {
  const h = toInt(heure);
  if (h === null) return null;
  if (h >= 6  && h < 12) return 'matin';
  if (h >= 12 && h < 18) return 'apres-midi';
  if (h >= 18 && h < 22) return 'soiree';
  return 'nuit';
}

function calculerTrimestre(mois) {
  const m = toInt(mois);
  if (!m) return null;
  return Math.ceil(m / 3);
}

// ─────────────────────────────────────────────
// EXTRACT — Lecture des CSV
// ─────────────────────────────────────────────

function lireCsv(chemin) {
  if (!fs.existsSync(chemin)) return null;
  const contenu = fs.readFileSync(chemin, 'utf-8').replace(/^\uFEFF/, '');
  return parse(contenu, {
    delimiter: ';',
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true
  });
}

function extract(annee) {
  // Cherche les fichiers : d'abord data/YYYY/, puis data/ (rétrocompatibilité 2022)
  const dirs = [
    path.join(DATA_DIR, String(annee)),
    DATA_DIR
  ];

  const trouver = (nom) => {
    for (const dir of dirs) {
      const chemin = path.join(dir, nom);
      const data   = lireCsv(chemin);
      if (data) return data;
    }
    throw new Error(`Fichier introuvable pour ${annee} : ${nom}\nLancez : npm run download`);
  };

  console.log(`  Lecture des CSV ${annee}...`);
  const caracteristiques = trouver(`caracteristiques-${annee}.csv`);
  const usagers          = trouver(`usagers-${annee}.csv`);
  const vehicules        = trouver(`vehicules-${annee}.csv`);

  console.log(`    ✓ ${caracteristiques.length} accidents, ${usagers.length} usagers, ${vehicules.length} véhicules`);
  return { caracteristiques, usagers, vehicules };
}

// ─────────────────────────────────────────────
// TRANSFORM
// ─────────────────────────────────────────────

function transform({ caracteristiques, usagers, vehicules }, annee) {

  const accidentsTransformes = caracteristiques.map(row => {
    const heure  = toStr(row.hrmn) ? String(row.hrmn).split(':')[0] : null;
    const minute = toStr(row.hrmn) ? String(row.hrmn).split(':')[1] : null;
    const mois   = toInt(row.mois);
    return {
      num_acc: toStr(row.Accident_Id || row.num_acc || row.Num_Acc),
      jour:    toInt(row.jour),
      mois,
      annee:   toInt(row.an) || annee,
      heure:   toInt(heure),
      minute:  toInt(minute),
      trimestre:       calculerTrimestre(mois),
      tranche_horaire: trancheHoraire(heure),
      departement:    toStr(row.dep),
      commune:        toStr(row.com),
      agglomeration:  toInt(row.agg),
      adresse:        toStr(row.adr),
      latitude:       toFloat(row.lat),
      longitude:      toFloat(row.long),
      luminosite:     toInt(row.lum),
      conditions_atmo: toInt(row.atm),
      type_intersection: toInt(row.int),
      type_collision:    toInt(row.col)
    };
  }).filter(a => a.num_acc !== null);

  const vehiculesTransformes = vehicules.map(row => ({
    num_acc: toStr(row.Accident_Id || row.num_acc || row.Num_Acc),
    num_veh: toStr(row.id_vehicule || row.num_veh || row.Num_Veh),
    categorie_vehicule: toInt(row.catv),
    sens:           toInt(row.sens),
    obstacle_fixe:  toInt(row.obs),
    point_choc:     toInt(row.choc),
    manoeuvre:      toInt(row.manv)
  })).filter(v => v.num_acc !== null && v.num_veh !== null);

  const usagersTransformes = usagers.map(row => {
    const sexe    = toInt(row.sexe);
    const gravite = toInt(row.grav);
    if (sexe !== 1 && sexe !== 2) return null;
    if (gravite === null || gravite < 1 || gravite > 4) return null;

    const anneeNaissance = toInt(row.an_nais);
    const anneeAccident  = toInt(row.an) || annee;

    return {
      num_acc: toStr(row.Accident_Id || row.num_acc || row.Num_Acc),
      num_veh: toStr(row.id_vehicule || row.num_veh || row.Num_Veh),
      place:           toInt(row.place),
      categorie_usager: toInt(row.catu),
      gravite,
      sexe,
      annee_naissance:    anneeNaissance,
      age_au_moment:      calculerAge(anneeAccident, anneeNaissance),
      type_trajet:        toInt(row.trajet),
      equipement_securite: toInt(row.secu1 || row.secu),
      annee_donnees:      annee
    };
  }).filter(u => u !== null);

  return { accidentsTransformes, vehiculesTransformes, usagersTransformes };
}

// ─────────────────────────────────────────────
// LOAD — Insertion dans SQLite
// ─────────────────────────────────────────────

function loadAnnee(db, data, annee, offsets) {
  const { accidentsTransformes, vehiculesTransformes, usagersTransformes } = data;
  let   { offsetTemps, offsetLieu, offsetVehicule, offsetUsager } = offsets;

  // Préparer les statements une seule fois (performance)
  const insTemps = db.prepare(`
    INSERT OR IGNORE INTO dim_temps
      (id_temps, jour, mois, annee, heure, minute, trimestre, tranche_horaire)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insLieu = db.prepare(`
    INSERT OR IGNORE INTO dim_lieu
      (id_lieu, departement, commune, agglomeration, adresse, latitude, longitude)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  const insAccident = db.prepare(`
    INSERT OR IGNORE INTO dim_accident
      (num_acc, id_temps, id_lieu, id_luminosite, id_atmosphere, type_intersection, type_collision)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  const insVehicule = db.prepare(`
    INSERT OR IGNORE INTO dim_vehicule
      (id_vehicule, num_acc, num_veh, id_categorie, sens, obstacle, choc, manoeuvre)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const insFait = db.prepare(`
    INSERT OR IGNORE INTO fait_usagers
      (id_usager, num_acc, id_vehicule, id_sexe, id_gravite, place,
       categorie_usager, annee_naissance, age_au_moment, type_trajet,
       equipement_securite, annee_donnees)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  // Index de recherche rapide : clé composite → id_vehicule
  const vehiculesRef = {};

  // Tout dans une seule transaction pour maximiser la vitesse
  const insertAll = db.transaction(() => {

    // dim_temps + dim_lieu + dim_accident (1 ligne par accident)
    accidentsTransformes.forEach((acc, idx) => {
      const idTemps = offsetTemps + idx + 1;
      const idLieu  = offsetLieu  + idx + 1;

      insTemps.run(
        idTemps, acc.jour, acc.mois, acc.annee,
        acc.heure, acc.minute, acc.trimestre, acc.tranche_horaire
      );

      insLieu.run(
        idLieu, acc.departement, acc.commune, acc.agglomeration,
        acc.adresse ? acc.adresse.substring(0, 500) : null,
        acc.latitude, acc.longitude
      );

      insAccident.run(
        acc.num_acc, idTemps, idLieu,
        LUMINOSITE_VALIDES.includes(acc.luminosite)       ? acc.luminosite        : null,
        ATMOSPHERE_VALIDES.includes(acc.conditions_atmo)  ? acc.conditions_atmo   : null,
        acc.type_intersection, acc.type_collision
      );
    });

    // dim_vehicule
    vehiculesTransformes.forEach((v, idx) => {
      const idVehicule = offsetVehicule + idx + 1;
      insVehicule.run(
        idVehicule, v.num_acc, v.num_veh,
        CATEGORIES_VALIDES.includes(v.categorie_vehicule) ? v.categorie_vehicule : null,
        v.sens, v.obstacle_fixe, v.point_choc, v.manoeuvre
      );
      vehiculesRef[`${v.num_acc}_${v.num_veh}`] = idVehicule;
    });

    // fait_usagers
    usagersTransformes.forEach((u, idx) => {
      const idUsager   = offsetUsager + idx + 1;
      const idVehicule = vehiculesRef[`${u.num_acc}_${u.num_veh}`] || null;
      insFait.run(
        idUsager, u.num_acc, idVehicule, u.sexe, u.gravite,
        u.place, u.categorie_usager, u.annee_naissance, u.age_au_moment,
        u.type_trajet, u.equipement_securite, u.annee_donnees
      );
    });
  });

  insertAll();

  return {
    nbAccidents: accidentsTransformes.length,
    nbVehicules: vehiculesTransformes.length,
    nbUsagers:   usagersTransformes.length
  };
}

// ─────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────

function main() {
  console.log('\n' + '█'.repeat(60));
  console.log('  CHARGEMENT SQLITE — ACCIDENTS DE LA ROUTE EN FRANCE');
  console.log('  Base locale : output/accidents.db');
  console.log('█'.repeat(60));

  // Créer output/ si nécessaire
  if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  // Supprimer l'ancienne base si elle existe (idempotent)
  if (fs.existsSync(DB_PATH)) {
    fs.unlinkSync(DB_PATH);
    console.log('\n  Ancienne base supprimée, recréation...');
  }

  // Ouvrir la base SQLite
  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');    // Meilleure perf en écriture
  db.pragma('synchronous = NORMAL');  // Bon compromis sécurité/vitesse
  db.pragma('foreign_keys = OFF');    // Désactivé pour les insertions en masse

  // Créer les tables
  console.log('\nCréation du schéma SQLite...');
  const schemaSql = fs.readFileSync(path.join(SQL_DIR, 'sqlite_schema.sql'), 'utf-8');
  db.exec(schemaSql);
  console.log('  ✓ Tables créées');

  // Insérer les dimensions statiques (données de référence)
  console.log('\nInsertion des dimensions statiques...');
  db.exec(`
    INSERT OR IGNORE INTO dim_sexe VALUES (1,'Masculin'),(2,'Féminin');

    INSERT OR IGNORE INTO dim_gravite VALUES
      (1,'Indemne',0),(2,'Tué',3),(3,'Hospitalisé',2),(4,'Blessé léger',1);

    INSERT OR IGNORE INTO dim_luminosite VALUES
      (1,'Plein jour'),(2,'Crépuscule ou aube'),
      (3,'Nuit sans éclairage public'),
      (4,'Nuit avec éclairage public non allumé'),
      (5,'Nuit avec éclairage public allumé');

    INSERT OR IGNORE INTO dim_atmosphere VALUES
      (1,'Normale'),(2,'Pluie légère'),(3,'Pluie forte'),
      (4,'Neige ou grêle'),(5,'Brouillard ou fumée'),
      (6,'Vent fort ou tempête'),(7,'Temps éblouissant'),
      (8,'Temps couvert'),(9,'Autre');
  `);

  const insCateg = db.prepare('INSERT OR IGNORE INTO dim_categorie_vehicule VALUES (?, ?)');
  const categories = [
    [1,'Bicyclette'],[2,'Cyclomoteur <50cm3'],[3,'Voiturette'],[7,'VL seul'],
    [10,'VU seul 1,5T<=PTAC<=3,5T'],[13,'PL seul 3,5T<PTCA<=7,5T'],[14,'PL seul > 7,5T'],
    [15,'PL > 3,5T + remorque'],[16,'Tracteur routier seul'],[17,'Tracteur routier + semi-remorque'],
    [20,'Engin spécial'],[21,'Tracteur agricole'],[30,'Scooter <50cm3'],
    [31,'Motocyclette >50cm3 et <=125cm3'],[32,'Scooter >50cm3 et <=125cm3'],
    [33,'Motocyclette >125cm3'],[34,'Scooter >125cm3'],[35,'Quad léger <=50cm3'],
    [36,'Quad lourd >50cm3'],[37,'Autobus'],[38,'Autocar'],[39,'Train'],[40,'Tramway'],
    [41,'Tricycle à moteur <=50cm3'],[42,'Tricycle à moteur >50cm3'],[43,'EDP à moteur'],[99,'Autre']
  ];
  db.transaction(() => categories.forEach(([id, lib]) => insCateg.run(id, lib)))();
  console.log('  ✓ Dimensions statiques OK');

  // Déterminer les années à charger selon les arguments CLI
  let annees;
  if (process.argv.includes('--all')) {
    // Toutes les années disponibles dans data/
    annees = [];
    for (let a = 2012; a <= 2022; a++) {
      const dossier   = path.join(DATA_DIR, String(a));
      const fichierRacine = path.join(DATA_DIR, `caracteristiques-${a}.csv`);
      if (fs.existsSync(path.join(dossier, `caracteristiques-${a}.csv`)) ||
          (a === 2022 && fs.existsSync(fichierRacine))) {
        annees.push(a);
      }
    }
    if (annees.length === 0) {
      console.error('\n  ✗ Aucune donnée trouvée. Lancez : npm run download');
      process.exit(1);
    }
  } else {
    const anneeArg = parseInt(process.argv[2]) || 2022;
    annees = [anneeArg];
  }

  console.log(`\nAnnées à charger : ${annees.join(', ')}`);

  // Traiter chaque année
  const debut = Date.now();
  let offsetTemps = 0, offsetLieu = 0, offsetVehicule = 0, offsetUsager = 0;

  for (const annee of annees) {
    console.log(`\n${'─'.repeat(50)}`);
    console.log(`  Traitement ${annee}...`);

    try {
      const brut      = extract(annee);
      const transforme = transform(brut, annee);
      const stats      = loadAnnee(db, transforme, annee, {
        offsetTemps, offsetLieu, offsetVehicule, offsetUsager
      });

      console.log(`  ✓ ${stats.nbAccidents} accidents | ${stats.nbVehicules} véhicules | ${stats.nbUsagers} usagers`);

      offsetTemps    += stats.nbAccidents;
      offsetLieu     += stats.nbAccidents;
      offsetVehicule += stats.nbVehicules;
      offsetUsager   += stats.nbUsagers;

    } catch (err) {
      console.error(`  ✗ Erreur ${annee} : ${err.message}`);
    }
  }

  db.close();

  const duree = Math.round((Date.now() - debut) / 1000);
  const tailleMB = (fs.statSync(DB_PATH).size / 1024 / 1024).toFixed(1);

  console.log('\n' + '═'.repeat(60));
  console.log('  CHARGEMENT SQLITE TERMINÉ');
  console.log('═'.repeat(60));
  console.log(`  Usagers total   : ${offsetUsager.toLocaleString('fr-FR')}`);
  console.log(`  Accidents total : ${offsetTemps.toLocaleString('fr-FR')}`);
  console.log(`  Durée           : ${duree}s`);
  console.log(`  Taille DB       : ${tailleMB} MB`);
  console.log(`  Fichier         : ${DB_PATH}`);
  console.log('\n  → Requêtes offline : npm run query:sqlite\n');
}

main();
