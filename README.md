# Projet ETL — Accidents de la Route en France 2022

**Web School Factory — SQL 4 Big Data — Cours de Gerard Toko**

> **Thématique :** Comparer la mortalité dans les accidents de voiture en France selon le sexe.

---

## Résultat clé

> **~75% des personnes tuées sur les routes françaises en 2022 sont des hommes.**
> Le taux de mortalité masculin (~3.2%) est environ **2 fois supérieur** au taux féminin (~1.6%).

---

## Technologies utilisées

| Composant        | Technologie                              |
|------------------|------------------------------------------|
| Scripting        | Node.js 18+                              |
| Base de données  | PostgreSQL (hébergée sur **Neon**)       |
| ORM              | Knex.js                                  |
| Parsing CSV      | csv-parse                                |
| Variables env    | dotenv                                   |
| Visualisation    | Chart.js (HTML/JS)                       |
| Source de données| data.gouv.fr — ONISR 2022                |

---

## Structure du projet

```
etl-accidents/
├── .env                       ← Connexion Neon (NON commité)
├── .gitignore
├── package.json
├── README.md
├── etl.js                     ← Pipeline ETL principal (Extract → Transform → Load)
├── knexfile.js                ← Configuration Knex.js
├── data/
│   ├── caracteristiques-2022.csv   ← Données accidents (à télécharger)
│   ├── usagers-2022.csv            ← Données usagers
│   └── vehicules-2022.csv          ← Données véhicules
├── sql/
│   ├── 01_schema.sql          ← Schéma Star Schema (CREATE TABLE + INDEX)
│   └── 02_analyses.sql        ← 8 requêtes analytiques SQL avancées
├── scripts/
│   ├── download.js            ← Téléchargement automatique des CSV
│   ├── run-queries.js         ← Exécution des 8 analyses SQL
│   ├── visualize.js           ← Mise à jour du dashboard avec données réelles
│   └── dashboard.html         ← Dashboard interactif (Chart.js)
└── output/
    └── *.json                 ← Résultats exportés des requêtes
```

---

## Lancer le projet

### Prérequis
- [Node.js 18+](https://nodejs.org) — vérifier avec `node --version`
- Un compte [Neon](https://neon.tech) gratuit (PostgreSQL cloud)

---

### 0. Cloner / ouvrir le projet

```bash
cd etl-accidents
```

---

### 1. Installer les dépendances

```bash
npm install
```

> Installe : `knex`, `pg`, `csv-parse`, `dotenv`

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
npm run download
```

Télécharge automatiquement les 3 fichiers depuis data.gouv.fr dans `data/` :
- `caracteristiques-2022.csv` (caractéristiques des accidents)
- `usagers-2022.csv` (personnes impliquées)
- `vehicules-2022.csv` (véhicules impliqués)

> **Si le téléchargement échoue**, téléchargez manuellement sur [data.gouv.fr](https://www.data.gouv.fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2024), année 2022, et placez les fichiers dans `data/`.

---

### 4. Lancer le pipeline ETL

```bash
npm run etl
```

Ce script effectue 3 phases :
1. **Extract** — lit les 3 CSV (~130 000 lignes)
2. **Transform** — nettoie les données, calcule les âges, valide les valeurs
3. **Load** — crée le star schema sur Neon et insère toutes les données par batches

Durée estimée : **2 à 5 minutes** selon la connexion internet.

---

### 5. Exécuter les analyses SQL

```bash
npm run analyze
```

Lance les 8 requêtes analytiques, affiche les résultats dans le terminal et sauvegarde les fichiers JSON dans `output/`.

---

### 6. Visualiser le dashboard

```bash
node scripts/visualize.js
```

Puis ouvrez `scripts/dashboard.html` dans votre navigateur (double-clic sur le fichier, ou via l'extension **Live Server** de VS Code).

Le dashboard affiche :
- Les KPIs clés (taux de mortalité, nombre de tués...)
- 5 graphiques interactifs (Chart.js)
- Un tableau récapitulatif complet par sexe

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
                    │  categorie_usager (1=cond/2=pass/3=piéton)│
                    │  age_au_moment (calculé)            │
                    │  type_trajet                        │
                    └────────────────────────────────────┘
```

---

## Requêtes analytiques (résumé)

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

---

## Exemples de résultats (2022)

### Taux de mortalité par sexe
| Sexe      | Total impliqués | Tués  | Taux mortalité | Part des tués |
|-----------|----------------|-------|----------------|---------------|
| Masculin  | 84 794         | 2 777 | **3.27%**      | **78.2%**     |
| Féminin   | 39 123         | 773   | **1.98%**      | **21.8%**     |

### Observations clés
- Les hommes représentent **68.4%** des usagers impliqués mais **78.2% des tués**
- Le taux de mortalité masculin (**3.27%**) est **1.65× celui des femmes** (1.98%)
- Nuit sans éclairage : taux de mortalité le plus élevé — **7.84%** chez les hommes
- Tranche d'âge la plus touchée chez les hommes : **+65 ans** (7.62%) et **18-25 ans** (2.97%)
- Piétons hommes : taux de **7.08%** — le rôle le plus dangereux
- Moto >125cm3 : **455 hommes tués** vs 39 femmes
- Pic de décès masculins : **juillet** (293 tués)

---

## Données sources

- **URL** : [data.gouv.fr — Accidents corporels](https://www.data.gouv.fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2024)
- **Producteur** : ONISR (Observatoire National Interministériel de la Sécurité Routière)
- **Licence** : Licence Ouverte / Open Licence
- **Séparateur CSV** : point-virgule (`;`)
- **Encodage** : UTF-8
- **Valeur inconnue** : `-1` (traité comme `NULL` lors de la transformation)
