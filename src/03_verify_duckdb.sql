-- Étape 4 : vérification indépendante sous DuckDB (lancée par src/03_verify.sh)
--
-- On ne réutilise RIEN de Spark : DuckDB relit le CSV BRUT sur S3 avec son propre lecteur CSV,
-- réapplique les règles métier (lignes exploitables, périodes, couverture >= 18 mois, causes
-- renormalisées) en SQL, puis compare son résultat au gold Spark (Parquet), liaison par liaison.
-- Sorties : processed/verification/comparaison_duckdb_spark.csv et resume.csv
-- La requête finale échoue (code retour != 0) si un écart dépasse 1e-6 point ou si le classement diffère.
.bail on
.timer on

CREATE OR REPLACE SECRET projet_s3 (
    TYPE s3, PROVIDER credential_chain, CHAIN 'env',          -- identifiants AWS_* d'Onyxia
    ENDPOINT getenv('AWS_S3_ENDPOINT'), URL_STYLE 'path', REGION 'us-east-1',
    SCOPE 's3://' || getenv('BUCKET') || '/');

SET VARIABLE raw = coalesce(nullif(getenv('RAW_URI'), ''), 's3://' || getenv('BUCKET')
    || '/bigdata_project/raw/regularite_tgv/extraction=2026-09/regularite-mensuelle-tgv-aqst.csv');
SET VARIABLE processed = coalesce(nullif(getenv('PROCESSED_URI'), ''), 's3://' || getenv('BUCKET')
    || '/bigdata_project/processed');
SELECT getvariable('raw') AS brut_lu, getvariable('processed') AS processed;

-- 1. brut (lecteur CSV DuckDB : BOM et champs multi-lignes gérés nativement)
CREATE TABLE brut AS SELECT * FROM read_csv(getvariable('raw'), delim = ';', header = true, quote = '"');
SELECT count(*) AS lignes_brutes_duckdb FROM brut;

-- 2. liaison x mois, lignes exploitables uniquement, services National/International sommés
CREATE TABLE lm AS
WITH b AS (
    SELECT *, upper(trim(gare_depart)) || ' -> ' || upper(trim(gare_arrivee)) AS liaison,
           coalesce(prct_cause_externe, 0) + coalesce(prct_cause_infra, 0) + coalesce(prct_cause_gestion_trafic, 0)
           + coalesce(prct_cause_materiel_roulant, 0) + coalesce(prct_cause_gestion_gare, 0)
           + coalesce(prct_cause_prise_en_charge_voyageurs, 0) AS s
    FROM brut
    WHERE nb_train_prevu > 0 AND nb_annulation <= nb_train_prevu
      AND nb_train_retard_arrivee <= nb_train_prevu - nb_annulation)
SELECT liaison, date,
       CASE WHEN date BETWEEN '2018-01' AND '2019-12' THEN 'avant'
            WHEN date BETWEEN '2024-01' AND '2025-12' THEN 'apres' END AS periode,
       sum(nb_train_prevu - nb_annulation) AS circ,
       sum(nb_train_retard_arrivee) AS ret,
       sum(nb_train_retard_arrivee * CASE WHEN s > 0 THEN coalesce(prct_cause_externe, 0) / s ELSE 0 END) AS r_externe,
       sum(nb_train_retard_arrivee * CASE WHEN s > 0 THEN coalesce(prct_cause_infra, 0) / s ELSE 0 END) AS r_infra,
       sum(nb_train_retard_arrivee * CASE WHEN s > 0 THEN coalesce(prct_cause_gestion_trafic, 0) / s ELSE 0 END) AS r_gestion_trafic,
       sum(nb_train_retard_arrivee * CASE WHEN s > 0 THEN coalesce(prct_cause_materiel_roulant, 0) / s ELSE 0 END) AS r_materiel_roulant,
       sum(nb_train_retard_arrivee * CASE WHEN s > 0 THEN coalesce(prct_cause_gestion_gare, 0) / s ELSE 0 END) AS r_gestion_gare,
       sum(nb_train_retard_arrivee * CASE WHEN s > 0 THEN coalesce(prct_cause_prise_en_charge_voyageurs, 0) / s ELSE 0 END) AS r_prise_en_charge_voyageurs,
       sum(nb_train_retard_arrivee * CASE WHEN s > 0 THEN 0 ELSE 1 END) AS r_non_renseignee
FROM b GROUP BY ALL HAVING periode IS NOT NULL;

-- 3. agrégats par période puis variation 2018-2019 -> 2024-2025
CREATE TABLE p AS
SELECT liaison, periode, count(DISTINCT date) AS n_mois, sum(circ) AS circ, sum(ret) AS ret,
       100 * (1 - sum(ret) / sum(circ)) AS ponct,
       100 * sum(r_externe) / sum(circ) AS externe, 100 * sum(r_infra) / sum(circ) AS infra,
       100 * sum(r_gestion_trafic) / sum(circ) AS gestion_trafic,
       100 * sum(r_materiel_roulant) / sum(circ) AS materiel_roulant,
       100 * sum(r_gestion_gare) / sum(circ) AS gestion_gare,
       100 * sum(r_prise_en_charge_voyageurs) / sum(circ) AS prise_en_charge_voyageurs,
       100 * sum(r_non_renseignee) / sum(circ) AS non_renseignee
FROM lm GROUP BY ALL;

CREATE TABLE verif AS
SELECT row_number() OVER (ORDER BY b.ponct - a.ponct, a.liaison) AS rang, a.liaison,
       a.circ AS circ_avant, b.circ AS circ_apres, a.ponct AS ponct_avant, b.ponct AS ponct_apres,
       b.ponct - a.ponct AS delta,
       b.externe - a.externe AS c_externe, b.infra - a.infra AS c_infra,
       b.gestion_trafic - a.gestion_trafic AS c_gestion_trafic,
       b.materiel_roulant - a.materiel_roulant AS c_materiel_roulant,
       b.gestion_gare - a.gestion_gare AS c_gestion_gare,
       b.prise_en_charge_voyageurs - a.prise_en_charge_voyageurs AS c_prise_en_charge_voyageurs,
       b.non_renseignee - a.non_renseignee AS c_non_renseignee
FROM p a JOIN p b ON a.liaison = b.liaison AND a.periode = 'avant' AND b.periode = 'apres'
WHERE a.n_mois >= 18 AND b.n_mois >= 18;

-- 4. comparaison avec le gold produit par Spark
CREATE TABLE spark AS SELECT * FROM read_parquet(getvariable('processed') || '/gold/degradation_liaisons/*.parquet');

CREATE TABLE comparaison AS
SELECT v.rang AS rang_duckdb, s.rang_degradation AS rang_spark, coalesce(v.liaison, s.liaison) AS liaison,
       round(v.ponct_avant, 3) AS ponct_avant_duckdb, round(s.ponctualite_avant, 3) AS ponct_avant_spark,
       round(v.ponct_apres, 3) AS ponct_apres_duckdb, round(s.ponctualite_apres, 3) AS ponct_apres_spark,
       round(v.delta, 3) AS delta_duckdb, round(s.delta_ponctualite_pts, 3) AS delta_spark,
       abs(v.circ_avant - s.trains_circules_avant) + abs(v.circ_apres - s.trains_circules_apres) AS ecart_trains,
       greatest(abs(v.ponct_avant - s.ponctualite_avant), abs(v.ponct_apres - s.ponctualite_apres),
                abs(v.delta - s.delta_ponctualite_pts)) AS ecart_ponct_pts,
       greatest(abs(v.c_externe - s.contrib_externe_pts), abs(v.c_infra - s.contrib_infra_pts),
                abs(v.c_gestion_trafic - s.contrib_gestion_trafic_pts),
                abs(v.c_materiel_roulant - s.contrib_materiel_roulant_pts),
                abs(v.c_gestion_gare - s.contrib_gestion_gare_pts),
                abs(v.c_prise_en_charge_voyageurs - s.contrib_prise_en_charge_voyageurs_pts),
                abs(v.c_non_renseignee - s.contrib_non_renseignee_pts)) AS ecart_causes_pts
FROM verif v FULL JOIN spark s ON v.liaison = s.liaison
ORDER BY rang_duckdb NULLS LAST;

CREATE TABLE resume AS
SELECT now()::TIMESTAMP AS verifie_le,
       (SELECT count(*) FROM brut) AS lignes_brutes,
       count(rang_duckdb) AS liaisons_duckdb, count(rang_spark) AS liaisons_spark,
       count(*) FILTER (WHERE rang_duckdb IS NOT NULL AND rang_spark IS NOT NULL) AS liaisons_communes,
       max(ecart_trains) AS ecart_max_trains,
       max(ecart_ponct_pts) AS ecart_max_ponctualite_pts,
       max(ecart_causes_pts) AS ecart_max_causes_pts,
       count(*) FILTER (WHERE rang_duckdb = rang_spark) AS rangs_identiques,
       bool_and(rang_duckdb = rang_spark) FILTER (WHERE rang_duckdb <= 10) AS top10_identique
FROM comparaison;

COPY comparaison TO (getvariable('processed') || '/verification/comparaison_duckdb_spark.csv') (HEADER);
COPY resume TO (getvariable('processed') || '/verification/resume.csv') (HEADER);

.mode duckbox
SELECT rang_duckdb AS rang_d, rang_spark AS rang_s, liaison, delta_duckdb, delta_spark,
       ecart_ponct_pts, ecart_causes_pts
FROM comparaison WHERE rang_duckdb <= 10 ORDER BY rang_duckdb;
.mode line
SELECT * FROM resume;
.mode duckbox
SELECT CASE WHEN liaisons_duckdb = liaisons_spark AND liaisons_communes = liaisons_spark
             AND ecart_max_trains = 0 AND ecart_max_ponctualite_pts < 1e-6 AND ecart_max_causes_pts < 1e-6
             AND rangs_identiques = liaisons_spark
            THEN 'VERIFICATION OK : DuckDB (depuis le brut) = Spark (gold) sur ' || liaisons_spark || ' liaisons'
            ELSE error('VERIFICATION KO : ecart entre DuckDB et Spark, voir verification/comparaison_duckdb_spark.csv')
       END AS verdict
FROM resume;
