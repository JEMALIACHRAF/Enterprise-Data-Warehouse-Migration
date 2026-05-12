# Documentation Technique — Pipeline Oracle → Snowflake

## 1. Architecture globale

```
Oracle ERP (Legacy)
      │
      │  JDBC partitionné (PySpark)
      ▼
GCS Landing Zone (Parquet / Snappy)
  gs://bucket/raw/schema/table/year=YYYY/month=MM/day=DD/
      │
      │  PySpark Transformations
      │  - Casting des types
      │  - SCD Type 2 (dimensions lentement variables)
      │  - Résolution des clés de substitution
      │  - Calcul amount_eur (FX normalization)
      ▼
Validation 3 couches
  1. Schéma (nullabilité, unicité)
  2. Règles métier (gross ≥ net, valeurs acceptées)
  3. Réconciliation (count + checksum Oracle ↔ Snowflake)
      │
      ▼
Snowflake DWH (Schéma en flocon)
  FINANCE_DWH.CORE.FACT_TRANSACTIONS
  FINANCE_DWH.CORE.DIM_* (géographie + produit + client + temps)
      │
      ▼
FINANCE_DWH.MARTS.V_* (vues analytiques)
```

---

## 2. Modèle dimensionnel — Schéma en flocon

### Pourquoi un schéma en flocon ?

Le schéma en flocon **normalise les dimensions** en les décomposant en sous-tables.
Contrairement au schéma en étoile où chaque dimension est une table plate, le schéma
en flocon réduit la redondance de données — ici **~30% d'espace en moins** comparé
à une dimension géographique plate.

### Dimension Géographie (décomposée en 4 niveaux)

```
DIM_CONTINENT          DIM_COUNTRY            DIM_REGION             DIM_CITY
──────────────         ───────────────────     ───────────────────     ─────────────────
continent_sk  ◄──────  continent_sk (FK)       region_sk       ◄───── region_sk (FK)
continent_code         country_sk       ◄───── country_sk (FK)        city_sk
continent_name         country_code            region_code             city_name
               ...     country_name            region_name             postal_code
                       currency_code           ...                     lat / lng
                       is_eu_member
```

### Dimension Produit (décomposée en 3 niveaux)

```
DIM_PRODUCT_LINE       DIM_PRODUCT_FAMILY      DIM_PRODUCT
──────────────         ───────────────────     ──────────────────────
product_line_sk ◄───── product_line_sk (FK)    product_family_sk (FK)
product_line_code      product_family_sk ◄───── product_sk
product_line_name      product_family_code     product_bk
business_unit          product_family_name     product_name
                       risk_category           isin_code
                       is_regulated            valid_from / valid_to   ← SCD Type 2
                                               is_current
```

### Table de faits

```
FACT_TRANSACTIONS
─────────────────────────────────────────────────────
time_sk          FK → DIM_TIME
customer_sk      FK → DIM_CUSTOMER
product_sk       FK → DIM_PRODUCT
city_sk          FK → DIM_CITY
transaction_bk   Clé métier Oracle (dégénérée)
transaction_type BUY / SELL / TRANSFER
gross_amount     Montant brut (devise originale)
net_amount       Montant net
fee_amount       Commissions
amount_eur       Montant net normalisé en EUR (net × FX rate)
source_system    ORACLE_ERP
batch_id         Identifiant du batch de chargement
loaded_at        Horodatage du chargement Snowflake
```

---

## 3. SCD Type 2 — Dimensions lentement variables

Les dimensions `DIM_PRODUCT` et `DIM_CUSTOMER` implémentent le **SCD Type 2** :
lors d'un changement de valeur (ex : nouveau nom de produit, changement de statut KYC),
l'ancien enregistrement est **expiré** et un nouvel enregistrement **courant** est créé.

```
product_sk  product_bk  product_name        valid_from   valid_to     is_current
──────────  ──────────  ──────────────────  ───────────  ───────────  ──────────
1           PROD001     BNP Paribas SA      2020-01-01   2024-06-14   FALSE      ← expiré
2           PROD001     BNP Paribas SA v2   2024-06-15   NULL         TRUE       ← courant
```

La fact table référence toujours le `product_sk` **au moment de la transaction**,
permettant une analyse historique précise.

---

## 4. Requêtes SQL utiles pour les analystes

### Revenue mensuel par pays
```sql
SELECT year, month_name, country_name, SUM(revenue_eur) AS ca_eur
FROM FINANCE_DWH_DEV.MARTS.V_MONTHLY_REVENUE_BY_COUNTRY
WHERE year = 2024
GROUP BY 1,2,3
ORDER BY 1,2, ca_eur DESC;
```

### Top 10 clients par volume
```sql
SELECT customer_name, customer_type, total_volume_eur, volume_rank
FROM FINANCE_DWH_DEV.MARTS.V_TOP_CLIENTS
WHERE volume_rank <= 10
ORDER BY volume_rank;
```

### KPIs qualité du pipeline
```sql
SELECT * FROM FINANCE_DWH_DEV.MARTS.V_PIPELINE_QUALITY_KPI;
```

### Analyse par canal (branch, online, API)
```sql
SELECT channel, transaction_status, nb_transactions, total_volume_eur
FROM FINANCE_DWH_DEV.MARTS.V_CHANNEL_ANALYSIS
WHERE year = 2024 AND quarter = 1
ORDER BY total_volume_eur DESC;
```

---

## 5. Bonnes pratiques Snowflake pour les analystes

### Toujours filtrer sur `is_current = TRUE` pour les dimensions SCD
```sql
-- ✅ Correct
SELECT * FROM DIM_PRODUCT WHERE is_current = TRUE;

-- ❌ Retourne toutes les versions historiques
SELECT * FROM DIM_PRODUCT;
```

### Utiliser `time_sk` (entier YYYYMMDD) pour filtrer la fact table
```sql
-- ✅ Efficient — utilise le cluster key de FACT_TRANSACTIONS
SELECT * FROM FACT_TRANSACTIONS WHERE time_sk BETWEEN 20240101 AND 20240131;

-- ❌ Moins efficient — conversion implicite
SELECT * FROM FACT_TRANSACTIONS WHERE loaded_at::DATE = '2024-01-15';
```

### Toujours joindre via les clés de substitution (SK), jamais les BK
```sql
-- ✅ Correct
SELECT ft.*, dp.product_name
FROM FACT_TRANSACTIONS ft
JOIN DIM_PRODUCT dp ON ft.product_sk = dp.product_sk AND dp.is_current;

-- ❌ Peut retourner des doublons si SCD Type 2 actif
SELECT ft.*, dp.product_name
FROM FACT_TRANSACTIONS ft
JOIN DIM_PRODUCT dp ON ft.product_sk = dp.product_sk;
```

---

## 6. Monitoring du pipeline

Vérifie le statut du dernier batch dans Snowflake :
```sql
SELECT
    batch_id,
    COUNT(*)         AS rows_loaded,
    MIN(loaded_at)   AS batch_start,
    MAX(loaded_at)   AS batch_end,
    DATEDIFF('minute', MIN(loaded_at), MAX(loaded_at)) AS duration_min
FROM FACT_TRANSACTIONS
GROUP BY batch_id
ORDER BY batch_start DESC
LIMIT 10;
```
