/**
 * Téléchargement des données d'accidents de la route
 * Source : data.gouv.fr - ONISR (Observatoire National Interministériel de la Sécurité Routière)
 *
 * Usage :
 *   npm run download          → télécharge 2022 uniquement (rétrocompatible)
 *   npm run download:all      → télécharge 2012 à 2022 (11 années)
 *   node scripts/download.js 2019  → télécharge une année spécifique
 *
 * Les fichiers sont sauvegardés dans :
 *   data/2022/caracteristiques-2022.csv (etc.)
 *   data/caracteristiques-2022.csv      (rétrocompatibilité 2022)
 */

const https = require('https');
const http  = require('http');
const fs    = require('fs');
const path  = require('path');

// ─────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────

const DATA_DIR = path.join(__dirname, '..', 'data');

// Identifiant du dataset sur data.gouv.fr
// URL de la page : https://www.data.gouv.fr/fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2024/
const DATASET_SLUG = 'bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2024';
const API_URL      = `https://www.data.gouv.fr/api/1/datasets/${DATASET_SLUG}/resources/?page_size=200`;

// Plage d'années disponibles
const ANNEE_MIN = 2012;
const ANNEE_MAX = 2022;

// Types de fichiers attendus par année
const TYPES = ['caracteristiques', 'usagers', 'vehicules'];

// URLs de secours connues pour 2022 (si l'API échoue)
const URLS_SECOURS_2022 = [
  {
    type: 'caracteristiques',
    url:  'https://static.data.gouv.fr/resources/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2021/20231005-093927/carcteristiques-2022.csv'
  },
  {
    type: 'usagers',
    url:  'https://static.data.gouv.fr/resources/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2021/20231005-094229/usagers-2022.csv'
  },
  {
    type: 'vehicules',
    url:  'https://static.data.gouv.fr/resources/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2021/20231005-094147/vehicules-2022.csv'
  }
];

// ─────────────────────────────────────────────
// UTILITAIRES
// ─────────────────────────────────────────────

/** Vérifie qu'un fichier existe avec une taille non nulle */
function fichierExiste(chemin) {
  try {
    return fs.statSync(chemin).size > 0;
  } catch {
    return false;
  }
}

/** Effectue un GET HTTP/HTTPS et retourne le corps de la réponse */
function fetchTexte(url) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https') ? https : http;
    client.get(url, { headers: { 'User-Agent': 'etl-accidents/1.0' } }, (res) => {
      if (res.statusCode === 301 || res.statusCode === 302) {
        return fetchTexte(res.headers.location).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        return reject(new Error(`HTTP ${res.statusCode} pour ${url}`));
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end',  () => resolve(Buffer.concat(chunks).toString('utf-8')));
      res.on('error', reject);
    }).on('error', reject);
  });
}

/** Télécharge un fichier vers une destination locale avec barre de progression */
function telechargerFichier(url, destination) {
  return new Promise((resolve, reject) => {
    const client   = url.startsWith('https') ? https : http;
    const tmpPath  = destination + '.tmp';
    const stream   = fs.createWriteStream(tmpPath);

    const requete = client.get(url, { headers: { 'User-Agent': 'etl-accidents/1.0' } }, (res) => {
      // Gestion des redirections
      if (res.statusCode === 301 || res.statusCode === 302) {
        stream.close();
        if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
        return telechargerFichier(res.headers.location, destination).then(resolve).catch(reject);
      }

      if (res.statusCode !== 200) {
        stream.close();
        if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
        return reject(new Error(`HTTP ${res.statusCode}`));
      }

      const total = parseInt(res.headers['content-length'] || '0', 10);
      let recu = 0;

      res.on('data', (chunk) => {
        recu += chunk.length;
        if (total > 0) {
          const pct = Math.round((recu / total) * 100);
          process.stdout.write(`\r    Progression : ${pct}% (${(recu / 1024 / 1024).toFixed(1)} MB)`);
        }
      });

      res.pipe(stream);
      stream.on('finish', () => {
        process.stdout.write('\n');
        fs.renameSync(tmpPath, destination);
        resolve();
      });
    });

    requete.on('error', (err) => {
      if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
      reject(err);
    });
    stream.on('error', (err) => {
      if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
      reject(err);
    });
  });
}

// ─────────────────────────────────────────────
// RÉCUPÉRATION DES URLs DEPUIS L'API data.gouv.fr
// ─────────────────────────────────────────────

/**
 * Interroge l'API data.gouv.fr pour trouver l'URL de téléchargement d'un fichier
 * @param {Array} resources - Liste des ressources retournées par l'API
 * @param {number} annee    - Année cible
 * @param {string} type     - Type de fichier (caracteristiques, usagers, vehicules)
 * @returns {string|null}   - URL du fichier ou null si introuvable
 */
function trouverUrl(resources, annee, type) {
  const motsCles = [String(annee), type.toLowerCase()];

  // Variantes orthographiques possibles dans les noms de fichiers
  const variantes = {
    'caracteristiques': ['caracteristiques', 'carcteristiques', 'caract']
  };

  return resources.find(r => {
    const titre = ((r.title || '') + ' ' + (r.url || '')).toLowerCase();
    const contientAnnee = titre.includes(String(annee));

    const mots = variantes[type] ? variantes[type] : [type];
    const contientType = mots.some(m => titre.includes(m));

    return contientAnnee && contientType;
  })?.url || null;
}

/**
 * Récupère la liste de toutes les ressources du dataset via l'API
 */
async function fetchResources() {
  console.log(`  Interrogation de l'API data.gouv.fr...`);
  try {
    const json = await fetchTexte(API_URL);
    const data = JSON.parse(json);
    // L'API peut renvoyer { data: [...] } ou directement un tableau
    return Array.isArray(data) ? data : (data.data || []);
  } catch (err) {
    console.warn(`  ⚠ API indisponible : ${err.message}`);
    return [];
  }
}

// ─────────────────────────────────────────────
// TÉLÉCHARGEMENT PAR ANNÉE
// ─────────────────────────────────────────────

async function telechargerAnnee(annee, resources) {
  // Créer le dossier data/YYYY/
  const dossierAnnee = path.join(DATA_DIR, String(annee));
  if (!fs.existsSync(dossierAnnee)) fs.mkdirSync(dossierAnnee, { recursive: true });

  let succes = 0;
  let echecs = 0;

  for (const type of TYPES) {
    const nomFichier  = `${type}-${annee}.csv`;
    const destination = path.join(dossierAnnee, nomFichier);

    // Copie rétrocompatible pour 2022 dans data/ (racine)
    const destinationRacine = annee === 2022 ? path.join(DATA_DIR, nomFichier) : null;

    // Si déjà présent, ignorer
    if (fichierExiste(destination)) {
      const tailleMB = (fs.statSync(destination).size / 1024 / 1024).toFixed(1);
      console.log(`    ✓ ${nomFichier} déjà présent (${tailleMB} MB)`);
      succes++;
      continue;
    }

    // Chercher l'URL via l'API, puis les URLs de secours pour 2022
    let url = trouverUrl(resources, annee, type);

    if (!url && annee === 2022) {
      const secours = URLS_SECOURS_2022.find(u => u.type === type);
      if (secours) {
        url = secours.url;
        console.log(`    ⚠ URL API introuvable pour ${type}-${annee}, utilisation de l'URL de secours`);
      }
    }

    if (!url) {
      console.error(`    ✗ URL introuvable pour ${nomFichier} (API sans résultat)`);
      echecs++;
      continue;
    }

    console.log(`    Téléchargement : ${nomFichier}`);
    try {
      await telechargerFichier(url, destination);
      const tailleMB = (fs.statSync(destination).size / 1024 / 1024).toFixed(1);
      console.log(`    ✓ ${nomFichier} téléchargé (${tailleMB} MB)`);

      // Copie rétrocompatible dans data/ pour 2022
      if (destinationRacine && !fichierExiste(destinationRacine)) {
        fs.copyFileSync(destination, destinationRacine);
      }

      succes++;
    } catch (err) {
      console.error(`    ✗ Échec : ${err.message}`);
      echecs++;
    }
  }

  return { succes, echecs };
}

// ─────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────

async function main() {
  // Déterminer les années à télécharger
  let annees;
  if (process.argv.includes('--all')) {
    annees = Array.from({ length: ANNEE_MAX - ANNEE_MIN + 1 }, (_, i) => ANNEE_MIN + i);
  } else {
    const anneeArg = parseInt(process.argv[2]) || 2022;
    annees = [anneeArg];
  }

  console.log('='.repeat(60));
  console.log('TÉLÉCHARGEMENT DES DONNÉES ACCIDENTS DE LA ROUTE');
  console.log(`Source : data.gouv.fr (ONISR) — Années : ${annees.join(', ')}`);
  console.log('='.repeat(60));

  // Créer data/ si nécessaire
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

  // Récupérer la liste des ressources une seule fois depuis l'API
  const resources = await fetchResources();
  console.log(`  ✓ ${resources.length} ressources trouvées dans le dataset`);

  // Télécharger chaque année
  let totalSucces = 0;
  let totalEchecs = 0;

  for (const annee of annees) {
    console.log(`\n${'─'.repeat(40)}`);
    console.log(`  ANNÉE ${annee}`);
    const { succes, echecs } = await telechargerAnnee(annee, resources);
    totalSucces += succes;
    totalEchecs += echecs;
  }

  // Résumé final
  console.log('\n' + '='.repeat(60));
  console.log(`RÉSULTAT : ${totalSucces} fichier(s) OK, ${totalEchecs} échec(s)`);

  if (totalEchecs > 0) {
    afficherInstructionsManuelle(annees);
    process.exit(1);
  } else {
    console.log(`✓ Tous les fichiers sont dans data/YYYY/`);
    console.log(`✓ Prochaine étape : npm run etl${annees.length > 1 ? ':all' : ''}`);
  }
}

function afficherInstructionsManuelle(annees) {
  console.log('\n' + '='.repeat(60));
  console.log('TÉLÉCHARGEMENT MANUEL — Instructions');
  console.log('='.repeat(60));
  console.log(`
1. Rendez-vous sur data.gouv.fr :
   https://www.data.gouv.fr/fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2024/

2. Dans l'onglet "Ressources", téléchargez pour chaque année :
   - caracteristiques-YYYY.csv
   - usagers-YYYY.csv
   - vehicules-YYYY.csv

3. Placez les fichiers dans :
   ${DATA_DIR}/YYYY/caracteristiques-YYYY.csv
   ${DATA_DIR}/YYYY/usagers-YYYY.csv
   ${DATA_DIR}/YYYY/vehicules-YYYY.csv

   (ou directement dans ${DATA_DIR}/ pour l'année 2022)

4. Relancez ensuite :
   npm run etl${annees.length > 1 ? ':all' : ''}
`);
}

main().catch((err) => {
  console.error('\nErreur fatale :', err.message);
  process.exit(1);
});
