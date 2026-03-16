# Projet ETL — Accidents de la Route en France 2012–2022

**Web School Factory — SQL 4 Big Data — Cours de Gerard Toko**

> **Thématique :** Comparer la mortalité dans les accidents de voiture en France selon le sexe, sur 11 années de données (2012–2022).

---

## Résultats clés

> **~75% des personnes tuées sur les routes françaises sont des hommes.**
> Le taux de mortalité masculin (~3.27%) est environ **1.65× supérieur** au taux féminin (~1.98%).
> La **Corse** et l'**Outre-Mer** affichent les taux de mortalité les plus élevés par région.
> Les décès ont baissé de **−29%** entre 2012 et 2022 (de 3 645 à 3 267 tués).

---

## Technologies utilisées

| Composant         | Technologie                                      |
|-------------------|--------------------------------------------------|
| Scripting         | Node.js 18+                                      |
| Base de données   | PostgreSQL (hébergée sur **Neon**)               |
| Base locale       | SQLite (`better-sqlite3`) — mode offline         |
| ORM               | Knex.js                                          |
| Parsing CSV       | csv-parse (auto-détection délimiteur `,` ou `;`) |
| Variables env     | dotenv                                           |
| Visualisation     | Chart.js — Dashboard HTML interactif             |
| Source de données | data.gouv.fr — ONISR 2012–2022                   |

---

## Structure du projet

```
etl-accidents/
├── .env                          ← Connexion Neon (NON commité)
├── .gitignore
├── package.json
├── README.md
├── etl.js                        ← Pipeline ETL principal (Extract → Transform → Load)
│                                    Modes : 2022 seul | --year=YYYY | --all (2012–2022)
├── knexfile.js                   ← Configuration Knex.js
├── data/
│   ├── 2012/                     ← Un dossier par année
│   │   ├── caracteristiques-2012.csv
│   │   ├── usagers-2012.csv
│   │   └── vehicules-2012.csv
│   ├── …/
│   └── 2022/
├── sql/
│   ├── 01_schema.sql             ← Schéma Star Schema (CREATE TABLE + INDEX)
│   └── 02_analyses.sql           ← 14 requêtes analytiques SQL avancées
├── scripts/
│   ├── download.js               ← Téléchargement automatique depuis data.gouv.fr
│   ├── run-queries.js            ← Exécution des 14 analyses SQL (PostgreSQL)
│   ├── load-sqlite.js            ← Chargement en SQLite local (offline)
│   ├── query-sqlite.js           ← Requêtes sur SQLite
│   ├── visualize.js              ← Mise à jour du dashboard avec données réelles
│   └── dashboard.html            ← Dashboard interactif multi-onglets (Chart.js)
└── output/
    └── *.json                    ← Résultats exportés des requêtes
```

---

## Lancer le projet

### Prérequis
- [Node.js 18+](https://nodejs.org) — vérifier avec `node --version`
- Un compte [Neon](https://neon.tech) gratuit (PostgreSQL cloud)

---

### 0. Cloner le projet

```bash
git clone https://github.com/axel-dlm/Projet-Big-data.git
cd Projet-Big-data
```

---

### 1. Installer les dépendances

```bash
npm install
```

---

### 2. Configurer la connexion base de données

1. Créez un projet sur [neon.tech](https://neon.tech)
2. Copiez votre **Connection string** (dashboard Neon → "Connect")
3. Collez-la dans le fichier `.env` à la racine :

```env
DATABASE_URL=postgresql://user:motdepasse@ep-xxxxx.us-east-2.aws.neon.tech/neondb?sslmode=require
```

---

### 3. Télécharger les données CSV

```bash
# 2022 uniquement (par défaut)
npm run download

# Toutes les années 2012–2022
npm run download:all
```

Télécharge automatiquement les 3 fichiers par année depuis data.gouv.fr :
- `caracteristiques-YYYY.csv` — caractéristiques des accidents
- `usagers-YYYY.csv` — personnes impliquées
- `vehicules-YYYY.csv` — véhicules impliqués

> **Si le téléchargement échoue**, téléchargez manuellement sur [data.gouv.fr](https://www.data.gouv.fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2024) et placez les fichiers dans `data/YYYY/`.

---

### 4. Lancer le pipeline ETL

```bash
# Année 2022 uniquement
npm run etl

# Toutes les années 2012–2022
npm run etl:all

# Une année spécifique
node etl.js --year=2019
```

3 phases :
1. **Extract** — lit les CSV avec auto-détection du délimiteur (`,` avant 2018, `;` après)
2. **Transform** — nettoie, calcule les âges, valide les coordonnées GPS
3. **Load** — crée le star schema sur Neon et insère par batches

#### Variante SQLite (sans connexion internet)

```bash
npm run load:sqlite        # 2022 uniquement
npm run load:sqlite:all    # 2012–2022
```

---

### 5. Exécuter les analyses SQL

```bash
# PostgreSQL
npm run analyze

# SQLite
npm run query:sqlite
```

Lance les 14 requêtes analytiques, affiche les résultats dans le terminal et sauvegarde les fichiers JSON dans `output/`.

---

### 6. Ouvrir le dashboard

```bash
open scripts/dashboard.html
```

---

## Dashboard interactif

Le dashboard `scripts/dashboard.html` s'ouvre directement dans le navigateur (aucun serveur requis).

**4 onglets :**

| Onglet | Contenu |
|--------|---------|
| **Vue d'ensemble** | KPIs, gravité par sexe, donut tués, évolution mensuelle, rôle, luminosité, type de route |
| **Par région** | Tableau trié/filtrable avec 15 régions, mini-barres de progression, badges de risque colorés |
| **Tendances 2012–2022** | Courbe évolution, taux H/F, impact COVID, tableau annuel avec deltas |
| **Profil des victimes** | Tranches d'âge, radar, horaires, type de véhicule, conditions météo |

---

## Modèle de données (Star Schema)

```
                    ┌──────────────┐
                    │  dim_temps   │
                    │  id_temps PK │
                    │  jour        │
                    │  mois        │
                    │  annee       │
                    │  trimestre   │
                    │  tranche_    │
                    │  horaire     │
                    └──────┬───────┘
                           │
┌─────────────┐     ┌──────▼───────┐     ┌───────────────┐
│  dim_sexe   │     │ dim_accident │     │   dim_lieu    │
│  id_sexe PK ├──┐  │  num_acc PK  ├──── │  id_lieu PK   │
│  libelle    │  │  │  id_temps FK │     │  departement  │
└─────────────┘  │  │  id_lieu FK  │     │  commune      │
                 │  │  id_luminosi │     │  latitude     │
┌─────────────┐  │  │  id_atmo FK  │     │  longitude    │
│ dim_gravite │  │  └──────┬───────┘     └───────────────┘
│ id_gravite  ├──┤         │
│ libelle     │  │  ┌──────▼───────┐     ┌───────────────────────┐
│ niveau_     │  │  │ dim_vehicule │     │  dim_categorie_vehicule│
│ severite    │  │  │ id_vehicule  ├──── │  id_categorie PK       │
└─────────────┘  │  │ num_acc FK   │     │  libelle               │
                 │  │ id_categorie │     └───────────────────────┘
                 │  └──────┬───────┘
                 │         │
                 │  ┌──────▼────────────────────────────┐
                 │  │          fait_usagers              │
                 └──┤  id_usager PK                      │
                    │  num_acc    FK → dim_accident       │
                    │  id_vehicule FK → dim_vehicule      │
                    │  id_sexe    FK → dim_sexe           │
                    │  id_gravite FK → dim_gravite        │
                    │  categorie_usager                   │
                    │  age_au_moment (calculé)            │
                    │  annee_donnees (2012–2022)          │
                    └────────────────────────────────────┘
```

---

## Requêtes analytiques (14 au total)

| # | Titre | Techniques SQL |
|---|-------|----------------|
| 1 | Victimes par sexe × gravité + % | Window Function `SUM OVER PARTITION` |
| 2 | Taux de mortalité par sexe | CTE, `NULLIF`, Window Function |
| 3 | Décès par tranche d'âge et sexe | `CASE WHEN`, multi-GROUP BY |
| 4 | Mortalité par luminosité | JOIN multi-tables, `HAVING` |
| 5 | Saisonnalité (décès par mois) | JOIN sur dim_temps |
| 6 | Mortalité par rôle (cond/pass/piéton) | `CASE WHEN`, Window Function |
| 7 | Top véhicules par sexe | `RANK() OVER PARTITION`, sous-requête |
| 8 | Tableau récapitulatif complet | 4 CTEs enchaînées, `CROSS JOIN` |
| 9 | Évolution annuelle par sexe | `GROUP BY annee_donnees` |
| 10 | Total tués par année | Agrégation temporelle |
| 11 | Top années les plus meurtrières | `RANK() OVER` |
| 12 | Évolution du ratio H/F | Window Function sur séries temporelles |
| 13 | Avant / Pendant / Après COVID | `CASE WHEN` sur périodes |
| 14 | **Comparaison par région** | Mapping département → région, `FILTER WHERE` |

---

## Résultats clés par région (2022)

| Région | Tués | Taux mortalité | Taux H | Taux F |
|--------|------|----------------|--------|--------|
| Corse | 34 | 3.47% | 3.88% | 2.58% |
| Outre-Mer | 130 | 3.01% | 3.32% | 2.34% |
| Centre-Val de Loire | 163 | 2.90% | 3.24% | 2.20% |
| Île-de-France | 296 | 1.39% | 1.60% | 0.97% |

---

## Données sources

- **URL** : [data.gouv.fr — Accidents corporels](https://www.data.gouv.fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2024)
- **Producteur** : ONISR (Observatoire National Interministériel de la Sécurité Routière)
- **Licence** : Licence Ouverte / Open Licence
- **Années disponibles** : 2012 à 2022 (11 années)
- **Séparateur CSV** : `,` (avant 2018) ou `;` (2018 et après) — auto-détecté
- **Encodage** : UTF-8
- **Valeur inconnue** : `-1` (traité comme `NULL` lors de la transformation)
