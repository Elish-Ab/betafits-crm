/* ----------------------------------------------------------------------
   dim_prospects_sf Quarterly Refresh Script
   PURPOSE:
     - Refresh all computed fields in dim_prospects_sf
     - Use staging for atomic, zero-downtime updates
     - Includes ONLY logic explicitly defined in your PDF
   ---------------------------------------------------------------------- */

BEGIN;

/* ======================================================================
   1. CREATE STAGING TABLE
   ====================================================================== */

DROP TABLE IF EXISTS stg_dim_prospects_sf;

CREATE TEMP TABLE stg_dim_prospects_sf AS
SELECT *
FROM f_5500.dim_prospects_sf;

/* Add index for fast joins */
CREATE INDEX ON stg_dim_prospects_sf(ack_id);


/* ======================================================================
   2. BASE FIELDS FROM f_5500_sf_2023
   ====================================================================== */

UPDATE stg_dim_prospects_sf d
SET 
    plan_year_begin_date      = f.sf_plan_year_begin_date,
    plan_name                 = f.sf_plan_name,
    sf_type_pension_bnft_code = f.sf_type_pension_bnft_code,
    sf_type_welfare_bnft_code = f.sf_type_welfare_bnft_code,
    sf_admin_name_same_spon_ind = f.sf_admin_name_same_spon_ind,
    sf_plan_eff_date          = f.sf_plan_eff_date,
    sf_initial_filing_ind     = f.sf_initial_filing_ind,
    sf_admin_name             = f.sf_admin_name,
    opin_letter_serial_num    = f.sf_opin_letter_serial_num
FROM f_5500.f_5500_sf_2023 f
WHERE d.ack_id = f.ack_id;


/* ======================================================================
   3. FORM PLAN YEAR (from PLAN_YEAR_BEGIN_DATE)
   ====================================================================== */

UPDATE stg_dim_prospects_sf
SET form_plan_year = EXTRACT(YEAR FROM TO_DATE(plan_year_begin_date, 'YYYY-MM-DD'))
WHERE plan_year_begin_date IS NOT NULL;


/* ======================================================================
   4. PARTICIPATION METRICS
      Using:
        - SF_TOT_ACT_PARTCP_EOY_CNT
        - SF_PARTCP_ACCOUNT_BAL_CNT
        - SF_TOT_ACT_RTD_SEP_BENEF_CNT
   ====================================================================== */

UPDATE stg_dim_prospects_sf d
SET 
    eligible_participants = s.sf_tot_act_partcp_eoy_cnt,
    with_balances         = s.sf_partcp_account_bal_cnt,
    separated             = s.sf_tot_act_rtd_sep_benef_cnt - s.sf_tot_act_partcp_eoy_cnt,
    current_participating =
        s.sf_partcp_account_bal_cnt - (s.sf_tot_act_rtd_sep_benef_cnt - s.sf_tot_act_partcp_eoy_cnt),
    participation_rate =
        CASE WHEN s.sf_tot_act_partcp_eoy_cnt = 0 THEN NULL
             ELSE (s.sf_partcp_account_bal_cnt - (s.sf_tot_act_rtd_sep_benef_cnt - s.sf_tot_act_partcp_eoy_cnt))::NUMERIC
                   / NULLIF(s.sf_tot_act_partcp_eoy_cnt, 0)
        END
FROM f_5500.f_5500_sf_2023 s
WHERE d.ack_id = s.ack_id;


/* ======================================================================
   5. ADDITIONAL METRICS FROM PDF
   ====================================================================== */

UPDATE stg_dim_prospects_sf d
SET 
    avg_account_balance =
        CASE WHEN s.sf_partcp_account_bal_cnt = 0 THEN NULL
             ELSE s.sf_net_assets_eoy_amt::NUMERIC / NULLIF(s.sf_partcp_account_bal_cnt, 0)
        END,

    er_effective_contrib_pct =
        CASE WHEN (s.sf_emplr_contrib_income_amt + s.sf_particip_contrib_income_amt) = 0 THEN NULL
             ELSE s.sf_emplr_contrib_income_amt::NUMERIC
                / NULLIF(s.sf_emplr_contrib_income_amt + s.sf_particip_contrib_income_amt, 0)
        END,

    loans_per_active_with_bal =
        CASE WHEN (s.sf_partcp_account_bal_cnt -
                   (s.sf_tot_act_rtd_sep_benef_cnt - s.sf_tot_act_partcp_eoy_cnt)) = 0 THEN NULL
             ELSE s.sf_partcp_loans_eoy_amt::NUMERIC /
                  NULLIF((s.sf_partcp_account_bal_cnt -
                   (s.sf_tot_act_rtd_sep_benef_cnt - s.sf_tot_act_partcp_eoy_cnt)), 0)
        END,

    fidelity_bond_pct =
        CASE WHEN s.sf_net_assets_boy_amt = 0 THEN NULL
             ELSE s.sf_plan_ins_fdlty_bond_amt::NUMERIC / NULLIF(s.sf_net_assets_boy_amt, 0)
        END

FROM f_5500.f_5500_sf_2023 s
WHERE d.ack_id = s.ack_id;


/* ======================================================================
   6. ADMIN & SPONSOR SIGNER LOGIC
   ====================================================================== */

UPDATE stg_dim_prospects_sf d
SET admin_signer_name =
    CASE WHEN s.valid_admin_signature IS NOT NULL
         THEN s.sf_admin_signed_name
         ELSE s.sf_admin_manual_signed_name
    END,
    spons_signer_name =
    CASE WHEN s.valid_sponsor_signature IS NOT NULL
         THEN s.sf_spons_signed_name
         ELSE s.sf_spons_manual_signed_name
    END
FROM f_5500.f_5500_sf_2023 s
WHERE d.ack_id = s.ack_id;


/* ======================================================================
   7. BOOLEAN FLAGS BASED ON BENEFIT CODE RULES
   ====================================================================== */

/* 7.1 Health Benefits */
UPDATE stg_dim_prospects_sf
SET contains_health_benefits =
    sf_type_welfare_bnft_code IS NOT NULL
    AND sf_type_welfare_bnft_code LIKE '%4A%';

/* 7.2 Defined Contribution */
UPDATE stg_dim_prospects_sf
SET contains_defined_contribution =
    sf_type_pension_bnft_code IS NOT NULL
    AND sf_type_pension_bnft_code LIKE '%2%';

/* 7.3 Contains 403b */
UPDATE stg_dim_prospects_sf
SET contains_403b =
    sf_type_pension_bnft_code IS NOT NULL
    AND sf_type_pension_bnft_code LIKE '%2%'
    AND (sf_type_pension_bnft_code LIKE '%2L%' OR sf_type_pension_bnft_code LIKE '%2M%');

/* 7.4 ESOP */
UPDATE stg_dim_prospects_sf
SET contains_esop =
    sf_type_pension_bnft_code IS NOT NULL
    AND sf_type_pension_bnft_code LIKE '%2%'
    AND (sf_type_pension_bnft_code LIKE '%2O%' OR
         sf_type_pension_bnft_code LIKE '%2P%' OR
         sf_type_pension_bnft_code LIKE '%2Q%');

/* 7.5 Cash Balance */
UPDATE stg_dim_prospects_sf
SET contains_cash_balance_plan =
    sf_type_pension_bnft_code IS NOT NULL
    AND sf_type_pension_bnft_code LIKE '%1%'
    AND sf_type_pension_bnft_code LIKE '%1C%';

/* 7.6 Defined Benefit */
UPDATE stg_dim_prospects_sf
SET contains_defined_benefit =
    sf_type_pension_bnft_code IS NOT NULL
    AND sf_type_pension_bnft_code LIKE '%1%';

/* 7.7 New Comparability */
UPDATE stg_dim_prospects_sf
SET contains_new_comparability =
    sf_type_pension_bnft_code IS NOT NULL
    AND sf_type_pension_bnft_code LIKE '%2A%';

/* 7.8 401k */
UPDATE stg_dim_prospects_sf
SET contains_401k =
    sf_type_pension_bnft_code IS NOT NULL
    AND sf_type_pension_bnft_code LIKE '%2J%';

/* 7.9 Automatic Enrollment */
UPDATE stg_dim_prospects_sf
SET contains_automatic_enrollment =
    sf_type_pension_bnft_code IS NOT NULL
    AND sf_type_pension_bnft_code LIKE '%2S%';

/* 7.10 Controlled Group */
UPDATE stg_dim_prospects_sf
SET contains_controlled_group =
    sf_type_pension_bnft_code IS NOT NULL
    AND sf_type_pension_bnft_code LIKE '%3H%';
	
/* 7.11 Brokerage Accounts */
UPDATE stg_dim_prospects_sf
SET contains_controlled_group =
    sf_type_pension_bnft_code IS NOT NULL
    AND sf_type_pension_bnft_code LIKE '%2R%';

/* ======================================================================
   8. FEE & CONTRIBUTION METRICS
      (From long update block in PDF)
   ====================================================================== */

UPDATE stg_dim_prospects_sf fx
SET
    est_investment_return =
        COALESCE(s.sf_other_income_amt, 0)
        / NULLIF((COALESCE(s.sf_net_assets_boy_amt,0) + COALESCE(s.sf_net_assets_eoy_amt,0)) / 2, 0),

    est_direct_fees =
        (COALESCE(s.sf_admin_srvc_providers_amt, 0) + COALESCE(s.sf_oth_expenses_amt, 0))
        / NULLIF((COALESCE(s.sf_net_assets_boy_amt,0) + COALESCE(s.sf_net_assets_eoy_amt,0)) / 2, 0),

    est_indirect_fees =
        COALESCE(s.sf_broker_fees_paid_amt, 0)
        / NULLIF((COALESCE(s.sf_net_assets_boy_amt,0) + COALESCE(s.sf_net_assets_eoy_amt,0)) / 2, 0),

    corrective_distributions_ind =
        COALESCE(s.sf_corrective_deemed_distr_amt, 0) > 0,

    fail_transmit_contrib_ind =
        COALESCE(s.sf_fail_transmit_contrib_amt, 0) > 0,

    partcp_loans_ind =
        COALESCE(s.sf_partcp_loans_eoy_amt, 0) > 0,

    direct_fees_usd =
        COALESCE(s.sf_admin_srvc_providers_amt, 0)
        + COALESCE(s.sf_oth_expenses_amt, 0),

    total_fees_pct =
        (COALESCE(s.sf_admin_srvc_providers_amt, 0) + COALESCE(s.sf_oth_expenses_amt, 0))
            / NULLIF((COALESCE(s.sf_net_assets_boy_amt,0) +
                      COALESCE(s.sf_net_assets_eoy_amt,0)) / 2, 0)
        +
        COALESCE(s.sf_broker_fees_paid_amt, 0)
            / NULLIF((COALESCE(s.sf_net_assets_boy_amt,0) +
                      COALESCE(s.sf_net_assets_eoy_amt,0)) / 2, 0),

    total_fees_usd =
        COALESCE(s.sf_admin_srvc_providers_amt, 0)
        + COALESCE(s.sf_oth_expenses_amt, 0)
        + COALESCE(s.sf_broker_fees_paid_amt, 0),

    total_fees_pepm =
        (COALESCE(s.sf_admin_srvc_providers_amt, 0)
         + COALESCE(s.sf_oth_expenses_amt, 0)
         + COALESCE(s.sf_broker_fees_paid_amt, 0))
        / NULLIF((COALESCE(s.sf_partcp_account_bal_cnt, 0) -
                  (COALESCE(s.sf_tot_act_rtd_sep_benef_cnt, 0) -
                   COALESCE(s.sf_tot_act_partcp_eoy_cnt, 0))) * 12, 0),

    er_contrib_per_eligible_ee =
        COALESCE(s.sf_emplr_contrib_income_amt, 0)
        / NULLIF(COALESCE(s.sf_tot_act_partcp_boy_cnt, 0), 0),

    er_contrib_per_particip_ee =
        COALESCE(s.sf_emplr_contrib_income_amt, 0)
        / NULLIF((COALESCE(s.sf_partcp_account_bal_cnt, 0) -
                  (COALESCE(s.sf_tot_act_rtd_sep_benef_cnt, 0) -
                   COALESCE(s.sf_tot_act_partcp_eoy_cnt, 0))), 0),

    ee_contrib_per_eligible_ee =
        COALESCE(s.sf_particip_contrib_income_amt, 0)
        / NULLIF(COALESCE(s.sf_tot_act_partcp_boy_cnt, 0), 0),

    ee_contrib_per_particip_ee =
        COALESCE(s.sf_particip_contrib_income_amt, 0)
        / NULLIF((COALESCE(s.sf_partcp_account_bal_cnt, 0) -
                  (COALESCE(s.sf_tot_act_rtd_sep_benef_cnt, 0) -
                   COALESCE(s.sf_tot_act_partcp_eoy_cnt, 0))), 0)

FROM f_5500.f_5500_sf_2023 s
WHERE fx.ack_id = s.ack_id;


/* ======================================================================
   9. ATOMIC COMMIT — OVERWRITE PRODUCTION TABLE
   ====================================================================== */

DELETE FROM f_5500.dim_prospects_sf;
INSERT INTO f_5500.dim_prospects_sf
SELECT * FROM stg_dim_prospects_sf;

COMMIT;