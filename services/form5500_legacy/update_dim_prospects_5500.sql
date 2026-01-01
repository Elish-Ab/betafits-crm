/* ----------------------------------------------------------------------
   dim_prospects_5500 Quarterly Refresh Script
   PURPOSE:
     - Refresh all computed fields in dim_prospects_5500
     - Use staging tables to avoid downtime and ensure atomic updates
     - Apply all business logic from 5500 dataset only (NOT SF)
   ---------------------------------------------------------------------- */

BEGIN;

/* ======================================================================
   1. CREATE STAGING TABLE
   ====================================================================== */

DROP TABLE IF EXISTS stg_dim_prospects_5500;

CREATE TEMP TABLE stg_dim_prospects_5500 AS
SELECT *
FROM f_5500.dim_prospects_5500;

/* Index for faster update joins */
CREATE INDEX ON stg_dim_prospects_5500(ack_id);


/* ======================================================================
   2. CORE FIELD UPDATES FROM f_5500_2023
   ====================================================================== */

UPDATE stg_dim_prospects_5500 d
SET 
    form_plan_year_begin_date = f.form_plan_year_begin_date,
    form_plan_year             = EXTRACT(YEAR FROM TO_DATE(f.form_plan_year_begin_date,'YYYY-MM-DD')),
    plan_name                  = f.plan_name,
    filing_type =
        CASE 
            WHEN f.type_pension_bnft_code IS NULL OR f.type_pension_bnft_code = '' THEN 'H&W'
            ELSE '5500_R'
        END,
    type_welfare_bnft_code     = f.type_welfare_bnft_code,
    type_pension_bnft_code     = f.type_pension_bnft_code,
    admin_name_same_spon_ind   = f.admin_name_same_spon_ind,
    admin_name                 = f.admin_name,
    plan_eff_date              = f.plan_eff_date,
    initial_filing_ind         = f.initial_filing_ind
FROM f_5500.f_5500_2023 f
WHERE d.ack_id = f.ack_id;


/* ======================================================================
   3. PARTICIPATION METRICS
      Using:
        - TOT_ACTIVE_PARTCP_CNT
        - PARTCP_ACCOUNT_BAL_CNT
        - TOT_ACT_RTD_SEP_BENEF_CNT
   ====================================================================== */

UPDATE stg_dim_prospects_5500 d
SET 
    eligible_participants = f.tot_active_partcp_cnt,
    with_balances       = f.partcp_account_bal_cnt,
    separated           = f.tot_act_rtd_sep_benef_cnt - f.tot_active_partcp_cnt,
    current_participating =
        f.partcp_account_bal_cnt - (f.tot_act_rtd_sep_benef_cnt - f.tot_active_partcp_cnt),
    participation_rate =
        CASE WHEN f.tot_active_partcp_cnt = 0 THEN NULL
             ELSE (f.partcp_account_bal_cnt - (f.tot_act_rtd_sep_benef_cnt - f.tot_active_partcp_cnt))::NUMERIC
                   / NULLIF(f.tot_active_partcp_cnt,0)
        END
FROM f_5500.f_5500_2023 f
WHERE d.ack_id = f.ack_id;


/* ======================================================================
   4. NAICS CATEGORY MATCH
      - Match business_code from f_5500_2023 → naics_code in fct_naics_codes
   ====================================================================== */

UPDATE stg_dim_prospects_5500 d
SET naics_industry = n.industry_title
FROM f_5500.f_5500_2023 f
JOIN f_5500.fct_naics_codes n ON f.business_code = n.naics_code
WHERE d.ack_id = f.ack_id;


/* ======================================================================
   5. ADMIN & SPONSOR SIGNER LOGIC
      - Use VALID_* fields to decide between signed vs manual name
   ====================================================================== */

UPDATE stg_dim_prospects_5500 d
SET admin_signer_name =
    CASE WHEN f.valid_admin_signature IS NOT NULL
         THEN f.admin_signed_name
         ELSE f.admin_manual_signed_name
    END,
    spons_signer_name =
    CASE WHEN f.valid_sponsor_signature IS NOT NULL
         THEN f.spons_signed_name
         ELSE f.spons_manual_signed_name
    END
FROM f_5500.f_5500_2023 f
WHERE d.ack_id = f.ack_id;


/* ======================================================================
   6. BOOLEAN FLAGS BASED ON BENEFIT CODES
   ====================================================================== */

-- Contains health benefits
UPDATE stg_dim_prospects_5500
SET contains_health_benefits =
    type_welfare_bnft_code IS NOT NULL
    AND type_welfare_bnft_code LIKE '%4A%';

-- Defined contribution
UPDATE stg_dim_prospects_5500
SET contains_defined_contribution =
    type_pension_bnft_code IS NOT NULL
    AND type_pension_bnft_code LIKE '%2%';

-- 403(b)
UPDATE stg_dim_prospects_5500
SET contains_403b =
    type_pension_bnft_code IS NOT NULL
    AND type_pension_bnft_code LIKE '%2%'
    AND (type_pension_bnft_code LIKE '%2L%' OR type_pension_bnft_code LIKE '%2M%');

-- ESOP
UPDATE stg_dim_prospects_5500
SET contains_esop =
    type_pension_bnft_code IS NOT NULL
    AND type_pension_bnft_code LIKE '%2%'
    AND (
         type_pension_bnft_code LIKE '%2O%' OR
         type_pension_bnft_code LIKE '%2P%' OR
         type_pension_bnft_code LIKE '%2Q%'
    );

-- Cash balance
UPDATE stg_dim_prospects_5500
SET contains_cash_balance_plan =
    type_pension_bnft_code IS NOT NULL
    AND type_pension_bnft_code LIKE '%1%'
    AND type_pension_bnft_code LIKE '%1C%';

-- New comparability
UPDATE stg_dim_prospects_5500
SET contains_new_comparability =
    type_pension_bnft_code IS NOT NULL
    AND type_pension_bnft_code LIKE '%2A%';

-- 401(k)
UPDATE stg_dim_prospects_5500
SET contains_401k =
    type_pension_bnft_code IS NOT NULL
    AND type_pension_bnft_code LIKE '%2J%';

-- Automatic enrollment
UPDATE stg_dim_prospects_5500
SET contains_automatic_enrollment =
    type_pension_bnft_code IS NOT NULL
    AND type_pension_bnft_code LIKE '%2S%';

-- Controlled group
UPDATE stg_dim_prospects_5500
SET contains_controlled_group =
    type_pension_bnft_code IS NOT NULL
    AND type_pension_bnft_code LIKE '%3H%';

-- Defined benefit
UPDATE stg_dim_prospects_5500
SET contains_defined_benefit =
    type_pension_bnft_code IS NOT NULL
    AND type_pension_bnft_code LIKE '%1%';
	
-- Brokerage accounts
UPDATE stg_dim_prospects_5500
SET contains_defined_benefit =
    type_pension_bnft_code IS NOT NULL
    AND type_pension_bnft_code LIKE '%2R%';


/* ======================================================================
   7. FINALIZE — ATOMIC REPLACEMENT OF PRODUCTION TABLE
   ====================================================================== */

DELETE FROM f_5500.dim_prospects_5500;
INSERT INTO f_5500.dim_prospects_5500
SELECT * FROM stg_dim_prospects_5500;

COMMIT;