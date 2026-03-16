-- ============================================================
-- REQUÊTES EXPLORATOIRES — Cas où les femmes sont surreprésentées
-- Thématique : Nuancer l'analyse principale (78.2% des tués = hommes)
-- Chercher des contextes où les femmes ont un taux ≥ hommes
-- ============================================================


-- ============================================================
-- REQUÊTE A : Passagers vs conducteurs — les femmes passagères
--             sont-elles plus à risque que les hommes passagers ?
-- Hypothèse : les femmes sont souvent passagères (moins de contrôle)
-- Résultat attendu : taux de mortalité passagères > passagers
-- ============================================================

SELECT
    s.libelle                                               AS sexe,
    CASE fu.categorie_usager
        WHEN 1 THEN 'Conducteur'
        WHEN 2 THEN 'Passager'
        WHEN 3 THEN 'Piéton'
    END                                                     AS role,
    COUNT(*)                                                AS total,
    COUNT(*) FILTER (WHERE fu.id_gravite = 2)              AS nb_tues,
    ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        NULLIF(COUNT(*), 0), 2
    )                                                       AS taux_mortalite_pct,
    -- Comparer les deux sexes dans le même rôle via window function
    ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        NULLIF(SUM(COUNT(*) FILTER (WHERE fu.id_gravite = 2))
               OVER (PARTITION BY fu.categorie_usager), 0),
        1
    )                                                       AS part_tues_dans_role_pct
FROM fait_usagers fu
JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
WHERE fu.categorie_usager IN (1, 2, 3)
GROUP BY fu.id_sexe, s.libelle, fu.categorie_usager
ORDER BY fu.categorie_usager, fu.id_sexe;


-- ============================================================
-- REQUÊTE B : Piétonnes âgées (+65 ans)
--             Les femmes piétonnes de plus de 65 ans meurent-elles
--             proportionnellement plus que les hommes ?
-- Hypothèse : espérance de vie plus longue → plus de piétonnes âgées
--             mais aussi plus vulnérables aux chocs
-- ============================================================

SELECT
    s.libelle                                               AS sexe,
    CASE
        WHEN fu.age_au_moment BETWEEN 65 AND 74 THEN '65-74 ans'
        WHEN fu.age_au_moment BETWEEN 75 AND 84 THEN '75-84 ans'
        WHEN fu.age_au_moment >= 85              THEN '85 ans et +'
        ELSE 'Moins de 65 ans'
    END                                                     AS tranche_age_senior,
    COUNT(*)                                                AS total_pietons,
    COUNT(*) FILTER (WHERE fu.id_gravite = 2)              AS nb_tues,
    ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        NULLIF(COUNT(*), 0), 2
    )                                                       AS taux_mortalite_pct
FROM fait_usagers fu
JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
WHERE fu.categorie_usager = 3         -- piétons uniquement
  AND fu.age_au_moment >= 65
GROUP BY fu.id_sexe, s.libelle, tranche_age_senior
ORDER BY tranche_age_senior, fu.id_sexe;


-- ============================================================
-- REQUÊTE C : Type de collision — y a-t-il des configurations
--             où les femmes meurent plus que les hommes ?
-- Codes collision : 1=deux véhicules front, 2=arrière, 3=côté,
--   4=chaîne, 5=multiple côté, 6=autre multiple, 7=sans collision
-- Hypothèse : collisions frontales → taux mortalité femmes > hommes ?
-- ============================================================

SELECT
    s.libelle                                               AS sexe,
    CASE da.type_collision
        WHEN 1 THEN 'Deux véhicules - frontale'
        WHEN 2 THEN 'Deux véhicules - par l''arrière'
        WHEN 3 THEN 'Deux véhicules - par le côté'
        WHEN 4 THEN 'Trois véhicules et + - en chaîne'
        WHEN 5 THEN 'Trois véhicules et + - collisions multiples'
        WHEN 6 THEN 'Autre collision multiple'
        WHEN 7 THEN 'Sans collision'
        ELSE    'Inconnu'
    END                                                     AS type_collision,
    COUNT(*)                                                AS total,
    COUNT(*) FILTER (WHERE fu.id_gravite = 2)              AS nb_tues,
    ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        NULLIF(COUNT(*), 0), 2
    )                                                       AS taux_mortalite_pct
FROM fait_usagers fu
JOIN dim_sexe     s  ON fu.id_sexe = s.id_sexe
JOIN dim_accident da ON fu.num_acc = da.num_acc
WHERE da.type_collision IS NOT NULL
GROUP BY fu.id_sexe, s.libelle, da.type_collision
HAVING COUNT(*) > 200   -- éviter les petits groupes statistiquement peu fiables
ORDER BY da.type_collision, fu.id_sexe;


-- ============================================================
-- REQUÊTE D : Heure du trajet domicile-école (matin 7h-9h)
--             Les accidents scolaires touchent-ils plus les filles ?
-- Hypothèse : trajets école, accompagnement → profil différent
-- Code trajet : 2 = domicile-école, 1 = domicile-travail
-- ============================================================

WITH accidents_matin AS (
    SELECT
        fu.id_usager,
        fu.id_sexe,
        fu.id_gravite,
        fu.categorie_usager,
        fu.age_au_moment,
        dt.heure
    FROM fait_usagers fu
    JOIN dim_accident da ON fu.num_acc  = da.num_acc
    JOIN dim_temps    dt ON da.id_temps = dt.id_temps
    WHERE dt.heure BETWEEN 7 AND 9
      AND fu.age_au_moment <= 18
)
SELECT
    s.libelle                                               AS sexe,
    am.heure,
    COUNT(*)                                                AS total_jeunes,
    COUNT(*) FILTER (WHERE am.id_gravite = 2)              AS tues,
    COUNT(*) FILTER (WHERE am.id_gravite IN (2,3))         AS graves,
    ROUND(
        COUNT(*) FILTER (WHERE am.id_gravite = 2) * 100.0 /
        NULLIF(COUNT(*), 0), 2
    )                                                       AS taux_mortalite_pct
FROM accidents_matin am
JOIN dim_sexe s ON am.id_sexe = s.id_sexe
GROUP BY am.id_sexe, s.libelle, am.heure
ORDER BY am.heure, am.id_sexe;


-- ============================================================
-- REQUÊTE E : Absence d'équipement de sécurité
--             Les femmes sans ceinture meurent-elles plus ?
-- Code secu : 1=ceinture, 2=casque, 3=dispositif enfant,
--             4=gilet, 9=autre, 0=sans équipement
-- ============================================================

SELECT
    s.libelle                                               AS sexe,
    CASE fu.equipement_securite
        WHEN 1 THEN 'Ceinture'
        WHEN 2 THEN 'Casque'
        WHEN 3 THEN 'Dispositif enfant'
        WHEN 4 THEN 'Gilet réfléchissant'
        WHEN 9 THEN 'Autre'
        WHEN 0 THEN 'Aucun équipement'
        ELSE        'Non renseigné'
    END                                                     AS equipement,
    COUNT(*)                                                AS total,
    COUNT(*) FILTER (WHERE fu.id_gravite = 2)              AS nb_tues,
    ROUND(
        COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
        NULLIF(COUNT(*), 0), 2
    )                                                       AS taux_mortalite_pct
FROM fait_usagers fu
JOIN dim_sexe s ON fu.id_sexe = s.id_sexe
WHERE fu.equipement_securite IS NOT NULL
GROUP BY fu.id_sexe, s.libelle, fu.equipement_securite
HAVING COUNT(*) > 100
ORDER BY fu.equipement_securite, fu.id_sexe;


-- ============================================================
-- REQUÊTE F : Synthèse — Top 10 contextes où les femmes ont un
--             taux de mortalité SUPÉRIEUR ou ÉGAL aux hommes
-- Croisa : rôle × tranche d'âge × luminosité
-- ============================================================

WITH contextes AS (
    SELECT
        s.libelle                                           AS sexe,
        fu.id_sexe,
        CASE fu.categorie_usager
            WHEN 1 THEN 'Conducteur'
            WHEN 2 THEN 'Passager'
            WHEN 3 THEN 'Piéton'
            ELSE 'Autre'
        END                                                 AS role,
        CASE
            WHEN fu.age_au_moment < 18              THEN '-18 ans'
            WHEN fu.age_au_moment BETWEEN 18 AND 35 THEN '18-35 ans'
            WHEN fu.age_au_moment BETWEEN 36 AND 65 THEN '36-65 ans'
            WHEN fu.age_au_moment > 65              THEN '+65 ans'
            ELSE 'Âge inconnu'
        END                                                 AS tranche_age,
        l.libelle                                           AS luminosite,
        COUNT(*)                                            AS total,
        COUNT(*) FILTER (WHERE fu.id_gravite = 2)          AS nb_tues,
        ROUND(
            COUNT(*) FILTER (WHERE fu.id_gravite = 2) * 100.0 /
            NULLIF(COUNT(*), 0), 2
        )                                                   AS taux_mortalite_pct
    FROM fait_usagers fu
    JOIN dim_sexe       s  ON fu.id_sexe       = s.id_sexe
    JOIN dim_accident   da ON fu.num_acc       = da.num_acc
    JOIN dim_luminosite l  ON da.id_luminosite = l.id_luminosite
    WHERE fu.categorie_usager IN (1, 2, 3)
      AND fu.age_au_moment IS NOT NULL
    GROUP BY fu.id_sexe, s.libelle, fu.categorie_usager, tranche_age, da.id_luminosite, l.libelle
    HAVING COUNT(*) >= 50  -- groupes statistiquement significatifs
),
-- Comparer hommes vs femmes dans le même contexte
pivot AS (
    SELECT
        h.role,
        h.tranche_age,
        h.luminosite,
        h.total                                             AS total_hommes,
        h.nb_tues                                           AS tues_hommes,
        h.taux_mortalite_pct                                AS taux_hommes,
        f.total                                             AS total_femmes,
        f.nb_tues                                           AS tues_femmes,
        f.taux_mortalite_pct                                AS taux_femmes
    FROM contextes h
    JOIN contextes f ON h.role         = f.role
                    AND h.tranche_age  = f.tranche_age
                    AND h.luminosite   = f.luminosite
    WHERE h.id_sexe = 1   -- hommes
      AND f.id_sexe = 2   -- femmes
)
SELECT
    role,
    tranche_age,
    luminosite,
    taux_hommes,
    taux_femmes,
    ROUND(taux_femmes - taux_hommes, 2)                     AS ecart_femmes_moins_hommes,
    total_hommes,
    total_femmes
FROM pivot
WHERE taux_femmes >= taux_hommes  -- LES CAS OÙ LES FEMMES ≥ HOMMES
ORDER BY ecart_femmes_moins_hommes DESC
LIMIT 10;
