/**
 * Script de téléchargement des données d'accidents de la route
 * Source : data.gouv.fr - Bases de données annuelles des accidents corporels
 * Année : 2022
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');

// Dossier de destination des CSV
const DATA_DIR = path.join(__dirname, '..', 'data');

// URLs des fichiers CSV 2022 sur data.gouv.fr (vérifiées et fonctionnelles)
// Page du dataset : https://www.data.gouv.fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2024
const FICHIERS = [
  {
    nom: 'caracteristiques-2022.csv',
    url: 'https://static.data.gouv.fr/resources/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2021/20231005-093927/carcteristiques-2022.csv',
    description: 'Caractéristiques des accidents'
  },
  {
    nom: 'usagers-2022.csv',
    url: 'https://static.data.gouv.fr/resources/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2021/20231005-094229/usagers-2022.csv',
    description: 'Usagers impliqués dans les accidents'
  },
  {
    nom: 'vehicules-2022.csv',
    url: 'https://static.data.gouv.fr/resources/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2021/20231005-094147/vehicules-2022.csv',
    description: 'Véhicules impliqués dans les accidents'
  }
];

/**
 * Télécharge un fichier depuis une URL et le sauvegarde localement
 * @param {string} url - URL du fichier à télécharger
 * @param {string} destination - Chemin de destination local
 * @returns {Promise<void>}
 */
function telechargerFichier(url, destination) {
  return new Promise((resolve, reject) => {
    console.log(`  Téléchargement : ${url}`);

    const client = url.startsWith('https') ? https : http;
    const fichierTemp = destination + '.tmp';
    const writeStream = fs.createWriteStream(fichierTemp);

    const requete = client.get(url, (reponse) => {
      // Gestion des redirections HTTP
      if (reponse.statusCode === 301 || reponse.statusCode === 302) {
        writeStream.close();
        fs.unlinkSync(fichierTemp);
        console.log(`  Redirection vers : ${reponse.headers.location}`);
        telechargerFichier(reponse.headers.location, destination)
          .then(resolve)
          .catch(reject);
        return;
      }

      if (reponse.statusCode !== 200) {
        writeStream.close();
        fs.unlinkSync(fichierTemp);
        reject(new Error(`Erreur HTTP ${reponse.statusCode} pour ${url}`));
        return;
      }

      const tailleTotal = parseInt(reponse.headers['content-length'], 10);
      let telechargé = 0;

      reponse.on('data', (chunk) => {
        telechargé += chunk.length;
        if (tailleTotal) {
          const pourcentage = Math.round((telechargé / tailleTotal) * 100);
          process.stdout.write(`\r  Progression : ${pourcentage}% (${(telechargé / 1024 / 1024).toFixed(1)} MB)`);
        }
      });

      reponse.pipe(writeStream);

      writeStream.on('finish', () => {
        process.stdout.write('\n');
        // Renommer le fichier temporaire en fichier final
        fs.renameSync(fichierTemp, destination);
        resolve();
      });
    });

    requete.on('error', (err) => {
      writeStream.close();
      if (fs.existsSync(fichierTemp)) fs.unlinkSync(fichierTemp);
      reject(err);
    });

    writeStream.on('error', (err) => {
      if (fs.existsSync(fichierTemp)) fs.unlinkSync(fichierTemp);
      reject(err);
    });
  });
}

/**
 * Vérifie si un fichier existe déjà et a une taille non nulle
 * @param {string} chemin - Chemin du fichier
 * @returns {boolean}
 */
function fichierExiste(chemin) {
  try {
    const stat = fs.statSync(chemin);
    return stat.size > 0;
  } catch {
    return false;
  }
}

/**
 * Fonction principale de téléchargement
 */
async function main() {
  console.log('='.repeat(60));
  console.log('TÉLÉCHARGEMENT DES DONNÉES ACCIDENTS DE LA ROUTE 2022');
  console.log('Source : data.gouv.fr');
  console.log('='.repeat(60));

  // Créer le dossier data s'il n'existe pas
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    console.log(`Dossier créé : ${DATA_DIR}`);
  }

  let succes = 0;
  let echecs = 0;

  for (const fichier of FICHIERS) {
    const destination = path.join(DATA_DIR, fichier.nom);
    console.log(`\n[${fichier.description}]`);
    console.log(`  Fichier cible : ${fichier.nom}`);

    // Vérifier si le fichier existe déjà
    if (fichierExiste(destination)) {
      const stat = fs.statSync(destination);
      console.log(`  ✓ Déjà présent (${(stat.size / 1024 / 1024).toFixed(1)} MB) - ignoré`);
      succes++;
      continue;
    }

    try {
      await telechargerFichier(fichier.url, destination);
      const stat = fs.statSync(destination);
      console.log(`  ✓ Téléchargé avec succès (${(stat.size / 1024 / 1024).toFixed(1)} MB)`);
      succes++;
    } catch (err) {
      console.error(`  ✗ Échec : ${err.message}`);
      echecs++;
    }
  }

  console.log('\n' + '='.repeat(60));
  console.log(`RÉSULTAT : ${succes} fichier(s) OK, ${echecs} échec(s)`);

  if (echecs > 0) {
    console.log('\n⚠️  Certains fichiers n\'ont pas pu être téléchargés automatiquement.');
    afficherInstructionsManuelle();
  } else {
    console.log('✓ Tous les fichiers sont disponibles dans le dossier data/');
  }
}

/**
 * Affiche les instructions pour le téléchargement manuel
 */
function afficherInstructionsManuelle() {
  console.log('\n' + '='.repeat(60));
  console.log('INSTRUCTIONS POUR LE TÉLÉCHARGEMENT MANUEL');
  console.log('='.repeat(60));
  console.log(`
1. Rendez-vous sur data.gouv.fr :
   https://www.data.gouv.fr/fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation/

2. Dans l'onglet "Ressources", cherchez les fichiers pour l'année 2022 :
   - carcteristiques-2022.csv  → renommer en : caracteristiques-2022.csv
   - usagers-2022.csv          → garder ce nom
   - vehicules-2022.csv        → garder ce nom

3. Placez ces 3 fichiers dans le dossier :
   ${DATA_DIR}

4. Les fichiers utilisent :
   - Séparateur : point-virgule (;)
   - Encodage : UTF-8
   - En-têtes en première ligne

5. Relancez ensuite le ETL :
   npm run etl
`);
}

main().catch((err) => {
  console.error('Erreur fatale :', err.message);
  afficherInstructionsManuelle();
  process.exit(1);
});
