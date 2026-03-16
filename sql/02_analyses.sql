-- ============================================================
-- REQUÊTES ANALYTIQUES — Accidents de la Route en France 2022
-- Thématique : Comparaison de la mortalité par sexe
-- ============================================================
-- Techniques utilisées :
--   - JOINs multiples (INNER JOIN, LEFT JOIN)
--   - CTEs (WITH ... AS)
--   - Window Functions (OVER, PARTITION BY, RANK, SUM OVER)
--   - CASE WHEN / NULLIF / COALESCE
--   - GROUP BY, HAVING, ORDER BY
--   - Sous-requêtes
-- ============================================================


-- ============================================================
-- REQUÊTE 1 : Nombre de victimes par sexe et gravité
--             avec pourcentage par sexe (Window Function)
-- ============================================================
-- Objectif : Vue d'ensemble de la répartition sexe × gravité
-- Techniques : Window Function SUM OVER PARTITION, ROUND

SELECT
    s.libelle                                           AS sexe,
    g.libelle                                           AS gravite,
    g.niveau_severite,
    COUNT(*)                                            AS nb_victimes,
    -- Pourcentage dans le total de ce sexe (window function)
    ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (PARTITION BY fu.id_sexe),
        2
    )                                                   AS pct_dans_sexe,
    -- Pourcentage dans le total global
    ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (),
        2
    )                                                   AS pct_global
FROM fait_usagers fu
JOIN dim_sexe    s ON fu.id_sexe    = s.id_sexe
JOIN dim_gravite g ON fu.id_gravite = g.id_gravite
GROUP BY fu.id_sexe, s.libelle, fu.id_gravite, g.libelle, g.niveau_severite
ORDER BY fu.id_sexe, g.niveau_severite DESC;


-- ============================================================
-- REQUÊTE 2 : Taux de mortalité par sexe
--             (tués / total victimes × 100)
-- ============================================================
-- Objectif : Indicateur clé — qui meurt le plus ?
-- Techniques : CTE, CASE WHEN, NULLIF

WITH stats_sexe AS (
    SELECT
        fu.id_sexe,
        s.libelle                                           AS sexe,
        COUNT(*)                                            AS total_victimes,
        COUNT(*) FILTER (WHERE fu.id_gravite = 2)          AS nb_tues,
        COUNT(*) FILTER (WHERE fu.id_gravite = 3)          AS nb_hospitalises,
        COUNT(*) FILTER (WHERE fu.id_gravite = 4)          AS nb_blesses_legers,
        COUNT(*) FILTER (WHERE fu.id_gravite = 1)          AS nb_indemnes
    FROM fait_usagers fu
    JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
    GROUP BY fu.id_sexe, s.libelle
)
SELECT
    sexe,
    total_victimes,
    nb_tues,
    nb_hospitalises,
    nb_blesses_legers,
    nb_indemnes,
    ROUND(nb_tues * 100.0 / NULLIF(total_victimes, 0), 2) AS taux_mortalite_pct,
    ROUND(
        nb_tues * 100.0 / NULLIF(SUM(nb_tues) OVER (), 0),
        2
    )                                                       AS part_des_tues_pct
FROM stats_sexe
ORDER BY taux_mortalite_pct DESC;


-- ============================================================
-- REQUÊTE 3 : Décès par sexe et tranche d'âge
-- ============================================================
-- Objectif : Identifier les profils d'âge les plus à risque
-- Techniques : CASE WHEN (catégorisation), GROUP BY multi-niveaux

SELECT
    s.libelle                                           AS sexe,
    CASE
        WHEN fu.age_au_moment < 18              THEN 'Moins de 18 ans'
        WHEN fu.age_au_moment BETWEEN 18 AND 25 THEN '18-25 ans'
        WHEN fu.age_au_moment BETWEEN 26 AND 35 THEN '26-35 ans'
        WHEN fu.age_au_moment BETWEEN 36 AND 50 THEN '36-50 ans'
        WHEN fu.age_au_moment BETWEEN 51 AND 65 THEN '51-65 ans'
        WHEN fu.age_au_moment > 65              THEN 'Plus de 65 ans'
        ELSE 'Âge inconnu'
    END                                                 AS tranche_age,
    COUNT(*)                                            AS total_usagers,
    COUNT(*) FILTER (WHERE fu.id_gravite = 2)           AS nb_tues,
    ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        NULLIF(COUNT(*), 0),
        2
    )                                                   AS taux_mortalite_pct
FROM fait_usagers fu
JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
WHERE fu.age_au_moment IS NOT NULL
GROUP BY fu.id_sexe, s.libelle, tranche_age
ORDER BY fu.id_sexe,
    CASE tranche_age
        WHEN 'Moins de 18 ans' THEN 1
        WHEN '18-25 ans'       THEN 2
        WHEN '26-35 ans'       THEN 3
        WHEN '36-50 ans'       THEN 4
        WHEN '51-65 ans'       THEN 5
        WHEN 'Plus de 65 ans'  THEN 6
        ELSE 7
    END;


-- ============================================================
-- REQUÊTE 4 : Décès par sexe et luminosité (jour vs nuit)
-- ============================================================
-- Objectif : La nuit est-elle plus dangereuse pour un sexe ?
-- Techniques : JOIN dimension, GROUP BY, HAVING, ratio

SELECT
    s.libelle                                           AS sexe,
    l.libelle                                           AS luminosite,
    COUNT(fu.id_usager)                                 AS total_usagers,
    COUNT(*) FILTER (WHERE fu.id_gravite = 2)           AS nb_tues,
    ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        NULLIF(COUNT(fu.id_usager), 0),
        2
    )                                                   AS taux_mortalite_pct
FROM fait_usagers fu
JOIN dim_sexe     s  ON fu.id_sexe = s.id_sexe
JOIN dim_accident da ON fu.num_acc = da.num_acc
JOIN dim_luminosite l ON da.id_luminosite = l.id_luminosite
GROUP BY fu.id_sexe, s.libelle, da.id_luminosite, l.libelle
HAVING COUNT(fu.id_usager) > 50   -- Exclure les groupes trop petits
ORDER BY fu.id_sexe, taux_mortalite_pct DESC;


-- ============================================================
-- REQUÊTE 5 : Accidents par sexe et mois (saisonnalité)
-- ============================================================
-- Objectif : Y a-t-il un pic saisonnier différent selon le sexe ?
-- Techniques : JOIN multi-tables, ORDER BY, agrégations

SELECT
    s.libelle                                           AS sexe,
    dt.mois,
    TO_CHAR(
        TO_DATE(dt.mois::TEXT, 'MM'),
        'Month'
    )                                                   AS nom_mois,
    COUNT(fu.id_usager)                                 AS total_impliques,
    COUNT(*) FILTER (WHERE fu.id_gravite = 2)           AS nb_tues,
    COUNT(*) FILTER (WHERE fu.id_gravite = 3)           AS nb_hospitalises,
    ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        NULLIF(COUNT(fu.id_usager), 0),
        2
    )                                                   AS taux_mortalite_pct
FROM fait_usagers fu
JOIN dim_sexe     s  ON fu.id_sexe    = s.id_sexe
JOIN dim_accident da ON fu.num_acc    = da.num_acc
JOIN dim_temps    dt ON da.id_temps   = dt.id_temps
WHERE dt.mois IS NOT NULL
GROUP BY fu.id_sexe, s.libelle, dt.mois
ORDER BY fu.id_sexe, dt.mois;


-- ============================================================
-- REQUÊTE 6 : Gravité par sexe et rôle (conducteur/passager/piéton)
-- ============================================================
-- Objectif : Les hommes conducteurs meurent-ils plus que passagers ?
-- Techniques : CASE WHEN pour catégories, double GROUP BY

SELECT
    s.libelle                                           AS sexe,
    CASE fu.categorie_usager
        WHEN 1 THEN 'Conducteur'
        WHEN 2 THEN 'Passager'
        WHEN 3 THEN 'Piéton'
        ELSE 'Autre'
    END                                                 AS role,
    g.libelle                                           AS gravite,
    COUNT(*)                                            AS nb,
    ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (PARTITION BY fu.id_sexe, fu.categorie_usager),
        2
    )                                                   AS pct_dans_role_et_sexe
FROM fait_usagers fu
JOIN dim_sexe    s ON fu.id_sexe    = s.id_sexe
JOIN dim_gravite g ON fu.id_gravite = g.id_gravite
WHERE fu.categorie_usager IN (1, 2, 3)
GROUP BY fu.id_sexe, s.libelle, fu.categorie_usager, fu.id_gravite, g.libelle
ORDER BY fu.id_sexe, fu.categorie_usager, g.niveau_severite DESC;


-- ============================================================
-- REQUÊTE 7 : Accidents par sexe et type de véhicule
-- ============================================================
-- Objectif : Les hommes et femmes conduisent-ils des véhicules différents ?
-- Techniques : RANK() window function, sous-requête

WITH stats_vehicule AS (
    SELECT
        s.libelle                                       AS sexe,
        cv.libelle                                      AS type_vehicule,
        COUNT(fu.id_usager)                             AS total,
        COUNT(*) FILTER (WHERE fu.id_gravite = 2)       AS nb_tues,
        ROUND(
            COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
            NULLIF(COUNT(fu.id_usager), 0),
            2
        )                                               AS taux_mortalite_pct,
        RANK() OVER (
            PARTITION BY fu.id_sexe
            ORDER BY COUNT(fu.id_usager) DESC
        )                                               AS rang_par_sexe
    FROM fait_usagers fu
    JOIN dim_sexe              s  ON fu.id_sexe       = s.id_sexe
    JOIN dim_vehicule          dv ON fu.id_vehicule   = dv.id_vehicule
    JOIN dim_categorie_vehicule cv ON dv.id_categorie = cv.id_categorie
    GROUP BY fu.id_sexe, s.libelle, dv.id_categorie, cv.libelle
)
SELECT
    sexe,
    rang_par_sexe,
    type_vehicule,
    total,
    nb_tues,
    taux_mortalite_pct
FROM stats_vehicule
WHERE rang_par_sexe <= 8   -- Top 8 types de véhicules par sexe
ORDER BY sexe, rang_par_sexe;


-- ============================================================
-- REQUÊTE 8 : VUE RÉCAPITULATIVE COMPLÈTE (CTE + Window Functions)
-- ============================================================
-- Objectif : Tableau de bord complet pour la présentation
-- Techniques : Multiple CTEs enchaînées, toutes les Window Functions

WITH
-- CTE 1 : Comptages de base par sexe
base AS (
    SELECT
        fu.id_sexe,
        s.libelle                                           AS sexe,
        COUNT(*)                                            AS total_impliques,
        COUNT(*) FILTER (WHERE fu.id_gravite = 2)           AS nb_tues,
        COUNT(*) FILTER (WHERE fu.id_gravite = 3)           AS nb_hospitalises,
        COUNT(*) FILTER (WHERE fu.id_gravite = 4)           AS nb_blesses_legers,
        COUNT(*) FILTER (WHERE fu.id_gravite = 1)           AS nb_indemnes,
        COUNT(*) FILTER (WHERE fu.categorie_usager = 1)     AS nb_conducteurs,
        COUNT(*) FILTER (WHERE fu.categorie_usager = 2)     AS nb_passagers,
        COUNT(*) FILTER (WHERE fu.categorie_usager = 3)     AS nb_pietons,
        ROUND(AVG(fu.age_au_moment), 1)                     AS age_moyen,
        COUNT(DISTINCT fu.num_acc)                          AS nb_accidents_distincts
    FROM fait_usagers fu
    JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
    GROUP BY fu.id_sexe, s.libelle
),
-- CTE 2 : Calcul des totaux globaux pour les pourcentages
totaux AS (
    SELECT
        SUM(total_impliques) AS grand_total,
        SUM(nb_tues)         AS total_tues
    FROM base
),
-- CTE 3 : Agrégation des taux horaires (nuit vs jour)
horaires AS (
    SELECT
        fu.id_sexe,
        COUNT(*) FILTER (
            WHERE dt.tranche_horaire IN ('soiree', 'nuit')
            AND fu.id_gravite = 2
        )                                                   AS tues_nuit,
        COUNT(*) FILTER (
            WHERE dt.tranche_horaire IN ('matin', 'apres-midi')
            AND fu.id_gravite = 2
        )                                                   AS tues_jour
    FROM fait_usagers fu
    JOIN dim_accident da ON fu.num_acc  = da.num_acc
    JOIN dim_temps    dt ON da.id_temps = dt.id_temps
    GROUP BY fu.id_sexe
),
-- CTE 4 : Tranche d'âge modale (la plus fréquente parmi les tués)
age_modal AS (
    SELECT DISTINCT ON (fu.id_sexe)
        fu.id_sexe,
        CASE
            WHEN fu.age_au_moment < 18              THEN 'Moins de 18 ans'
            WHEN fu.age_au_moment BETWEEN 18 AND 25 THEN '18-25 ans'
            WHEN fu.age_au_moment BETWEEN 26 AND 35 THEN '26-35 ans'
            WHEN fu.age_au_moment BETWEEN 36 AND 50 THEN '36-50 ans'
            WHEN fu.age_au_moment BETWEEN 51 AND 65 THEN '51-65 ans'
            WHEN fu.age_au_moment > 65              THEN 'Plus de 65 ans'
        END                                                 AS tranche_age_modale
    FROM fait_usagers fu
    WHERE fu.id_gravite = 2
      AND fu.age_au_moment IS NOT NULL
    GROUP BY fu.id_sexe, tranche_age_modale
    ORDER BY fu.id_sexe, COUNT(*) DESC
)
-- Résultat final : tableau de bord complet
SELECT
    b.sexe,
    b.total_impliques,
    -- Pourcentage dans le total (window function)
    ROUND(b.total_impliques * 100.0 / t.grand_total, 1)    AS pct_du_total,
    b.nb_tues,
    -- Taux de mortalité
    ROUND(b.nb_tues * 100.0 / NULLIF(b.total_impliques, 0), 2)
                                                            AS taux_mortalite_pct,
    -- Part des tués de ce sexe sur les tués globaux
    ROUND(b.nb_tues * 100.0 / NULLIF(t.total_tues, 0), 1)  AS part_tues_pct,
    b.nb_hospitalises,
    ROUND(b.nb_hospitalises * 100.0 / NULLIF(b.total_impliques, 0), 2)
                                                            AS taux_hospitalisation_pct,
    b.nb_blesses_legers,
    b.nb_indemnes,
    b.nb_conducteurs,
    b.nb_passagers,
    b.nb_pietons,
    b.age_moyen,
    am.tranche_age_modale                                   AS tranche_age_tues_plus_freq,
    h.tues_nuit,
    h.tues_jour,
    ROUND(h.tues_nuit * 100.0 / NULLIF(b.nb_tues, 0), 1)  AS pct_tues_la_nuit,
    b.nb_accidents_distincts
FROM base b
CROSS JOIN totaux t
JOIN horaires  h  ON b.id_sexe = h.id_sexe
LEFT JOIN age_modal am ON b.id_sexe = am.id_sexe
ORDER BY b.id_sexe;


-- ============================================================
-- REQUÊTE 9 : Évolution du taux de mortalité H/F par année
-- ============================================================
-- Objectif : Voir si l'écart hommes/femmes se réduit sur 10 ans
-- Nécessite : données multi-années (npm run etl -- --all)
-- Techniques : GROUP BY multi-niveaux, Window Function par année

SELECT
    s.libelle                                               AS sexe,
    fu.annee_donnees                                        AS annee,
    COUNT(*)                                                AS total_victimes,
    COUNT(*) FILTER (WHERE fu.id_gravite = 2)              AS nb_tues,
    ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        NULLIF(COUNT(*), 0), 2
    )                                                       AS taux_mortalite_pct,
    -- Part des tués de ce sexe parmi tous les tués de l'année
    ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        NULLIF(
            SUM(COUNT(*) FILTER (WHERE fu.id_gravite = 2))
            OVER (PARTITION BY fu.annee_donnees),
            0
        ), 1
    )                                                       AS part_tues_annee_pct
FROM fait_usagers fu
JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
WHERE fu.annee_donnees IS NOT NULL
GROUP BY fu.id_sexe, s.libelle, fu.annee_donnees
ORDER BY fu.annee_donnees, fu.id_sexe;


-- ============================================================
-- REQUÊTE 10 : Nombre total de tués par année et par sexe
-- ============================================================
-- Objectif : Visualiser la courbe d'évolution sur 10 ans

SELECT
    fu.annee_donnees                                        AS annee,
    COUNT(*) FILTER (WHERE fu.id_sexe = 1 AND fu.id_gravite = 2)  AS tues_hommes,
    COUNT(*) FILTER (WHERE fu.id_sexe = 2 AND fu.id_gravite = 2)  AS tues_femmes,
    COUNT(*) FILTER (WHERE fu.id_gravite = 2)              AS total_tues,
    COUNT(*) FILTER (WHERE fu.id_sexe = 1)                 AS total_hommes,
    COUNT(*) FILTER (WHERE fu.id_sexe = 2)                 AS total_femmes,
    -- Taux par sexe sur l'année
    ROUND(
        COUNT(*) FILTER (WHERE fu.id_sexe = 1 AND fu.id_gravite = 2) * 100.0 /
        NULLIF(COUNT(*) FILTER (WHERE fu.id_sexe = 1), 0), 2
    )                                                       AS taux_mortalite_hommes,
    ROUND(
        COUNT(*) FILTER (WHERE fu.id_sexe = 2 AND fu.id_gravite = 2) * 100.0 /
        NULLIF(COUNT(*) FILTER (WHERE fu.id_sexe = 2), 0), 2
    )                                                       AS taux_mortalite_femmes
FROM fait_usagers fu
WHERE fu.annee_donnees IS NOT NULL
GROUP BY fu.annee_donnees
ORDER BY fu.annee_donnees;


-- ============================================================
-- REQUÊTE 11 : Année la plus meurtrière par sexe
-- ============================================================
-- Techniques : RANK() window function, sous-requête

WITH tues_par_annee AS (
    SELECT
        s.libelle                                           AS sexe,
        fu.annee_donnees                                    AS annee,
        COUNT(*) FILTER (WHERE fu.id_gravite = 2)          AS nb_tues,
        RANK() OVER (
            PARTITION BY fu.id_sexe
            ORDER BY COUNT(*) FILTER (WHERE fu.id_gravite = 2) DESC
        )                                                   AS rang
    FROM fait_usagers fu
    JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
    WHERE fu.annee_donnees IS NOT NULL
    GROUP BY fu.id_sexe, s.libelle, fu.annee_donnees
)
SELECT sexe, annee, nb_tues, rang
FROM tues_par_annee
WHERE rang <= 3   -- Top 3 années les plus meurtrières par sexe
ORDER BY sexe, rang;


-- ============================================================
-- REQUÊTE 12 : Évolution du ratio H/F parmi les tués
-- ============================================================
-- Objectif : Le ratio se réduit-il avec les années ?

WITH pivot AS (
    SELECT
        fu.annee_donnees                                    AS annee,
        COUNT(*) FILTER (WHERE fu.id_sexe = 1 AND fu.id_gravite = 2) AS tues_h,
        COUNT(*) FILTER (WHERE fu.id_sexe = 2 AND fu.id_gravite = 2) AS tues_f,
        COUNT(*) FILTER (WHERE fu.id_gravite = 2)          AS total_tues
    FROM fait_usagers fu
    WHERE fu.annee_donnees IS NOT NULL
    GROUP BY fu.annee_donnees
)
SELECT
    annee,
    tues_h,
    tues_f,
    total_tues,
    ROUND(tues_h * 100.0 / NULLIF(total_tues, 0), 1)       AS pct_hommes,
    ROUND(tues_f * 100.0 / NULLIF(total_tues, 0), 1)       AS pct_femmes,
    -- Ratio H/F : combien d'hommes pour 1 femme tuée
    ROUND(tues_h::NUMERIC / NULLIF(tues_f, 0), 2)          AS ratio_h_pour_1f,
    -- Évolution vs année précédente (window function LAG)
    ROUND(
        tues_h * 100.0 / NULLIF(total_tues, 0) -
        LAG(tues_h * 100.0 / NULLIF(total_tues, 0)) OVER (ORDER BY annee),
        1
    )                                                       AS variation_pct_hommes_vs_annee_prec
FROM pivot
ORDER BY annee;


-- ============================================================
-- REQUÊTE 13 : Comparaison avant / pendant / après COVID
-- ============================================================
-- Objectif : COVID-19 a-t-il eu un impact différent selon le sexe ?
-- 2019 = avant, 2020-2021 = COVID, 2022 = après

WITH periodes AS (
    SELECT
        fu.id_sexe,
        s.libelle                                           AS sexe,
        CASE
            WHEN fu.annee_donnees = 2019 THEN 'Avant COVID (2019)'
            WHEN fu.annee_donnees IN (2020, 2021) THEN 'Pendant COVID (2020-2021)'
            WHEN fu.annee_donnees = 2022 THEN 'Après COVID (2022)'
            ELSE 'Autre'
        END                                                 AS periode,
        fu.id_gravite,
        fu.annee_donnees
    FROM fait_usagers fu
    JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
    WHERE fu.annee_donnees IN (2019, 2020, 2021, 2022)
)
SELECT
    sexe,
    periode,
    COUNT(*)                                                AS total_victimes,
    COUNT(*) FILTER (WHERE id_gravite = 2)                 AS nb_tues,
    ROUND(
        COUNT(*) FILTER (WHERE id_gravite = 2) * 100.0 /
        NULLIF(COUNT(*), 0), 2
    )                                                       AS taux_mortalite_pct,
    COUNT(DISTINCT annee_donnees)                          AS nb_annees
FROM periodes
GROUP BY id_sexe, sexe, periode
ORDER BY id_sexe,
    CASE periode
        WHEN 'Avant COVID (2019)'          THEN 1
        WHEN 'Pendant COVID (2020-2021)'   THEN 2
        WHEN 'Après COVID (2022)'          THEN 3
        ELSE 4
    END;
