"""
Assumption Checks for Dutch Housing Crisis Statistical Analyses

Runs prerequisite assumption checks for all tests in analyze_statistics.py:
  1. Linear regression   → Shapiro-Wilk (residuals), Breusch-Pagan, Durbin-Watson
  2. One-way ANOVA       → Shapiro-Wilk (per group), Levene's test
  3. Independent t-test  → Shapiro-Wilk (per group), Levene's test
  4. Pearson correlation → Shapiro-Wilk (per variable), linearity note
  5. STL decomposition   → No formal checks needed (descriptive technique)

Output: results/0_assumption_checks.csv
        logs/assumption_checks.log
"""

import pandas as pd
import numpy as np
from scipy import stats
from sklearn.linear_model import LinearRegression
from statsmodels.stats.stattools import durbin_watson
from statsmodels.stats.diagnostic import het_breuschpagan
import statsmodels.api as sm
from pathlib import Path
import logging
import warnings

# ── Directory setup ──────────────────────────────────────────────────────────
Path('logs').mkdir(exist_ok=True)
Path('results').mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/assumption_checks.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore')

PROCESSED_DIR = Path('data/processed')
RESULTS_DIR   = Path('results')

# ── Helpers ───────────────────────────────────────────────────────────────────

def fmt_p(p: float) -> str:
    """Format p-value consistently with analyze_statistics.py."""
    if p < 1e-16:
        return "<1e-16"
    elif p < 0.001:
        return f"{p:.4e}"
    else:
        return f"{p:.6f}"


def shapiro_result(data: np.ndarray, label: str) -> dict:
    """
    Run Shapiro-Wilk normality test.
    Scipy caps at n=5000; for larger samples we use a random subsample
    and note this in the interpretation.
    """
    n = len(data)
    subsample_note = ""
    test_data = data

    if n > 5000:
        rng = np.random.default_rng(42)
        test_data = rng.choice(data, size=5000, replace=False)
        subsample_note = f" (subsample n=5000 of {n})"

    stat, p = stats.shapiro(test_data)
    normal = p >= 0.05

    interp = (
        f"Normal distribution assumed{subsample_note}"
        if normal
        else f"Non-normal distribution detected{subsample_note} — consider robust alternative"
    )
    return {
        'test': 'Shapiro-Wilk',
        'variable': label,
        'statistic': round(stat, 4),
        'p_value': fmt_p(p),
        'assumption_met': normal,
        'interpretation': interp,
    }


def levene_result(group1: np.ndarray, group2_or_groups, label1: str, label2: str) -> dict:
    """Run Levene's test for equality of variances (median-based, robust)."""
    if isinstance(group2_or_groups, list):
        stat, p = stats.levene(*group2_or_groups, center='median')
    else:
        stat, p = stats.levene(group1, group2_or_groups, center='median')

    equal_var = p >= 0.05
    interp = (
        "Equal variances assumed — standard test appropriate"
        if equal_var
        else "Unequal variances detected — use Welch correction"
    )
    return {
        'test': "Levene's",
        'variable': f"{label1} vs {label2}",
        'statistic': round(stat, 4),
        'p_value': fmt_p(p),
        'assumption_met': equal_var,
        'interpretation': interp,
    }


# ── Analysis-specific checks ─────────────────────────────────────────────────

def check_regression(df_doorloop: pd.DataFrame) -> list[dict]:
    """
    Analysis 1 — Linear regression assumptions:
      - Normality of residuals (Shapiro-Wilk)
      - Homoscedasticity (Breusch-Pagan)
      - No autocorrelation (Durbin-Watson)
    """
    logger.info("  [1] Linear regression assumptions")
    records = []

    yearly = (
        df_doorloop
        .dropna(subset=['Doorlooptijd_Mediaan'])
        .groupby('Jaar')['Doorlooptijd_Mediaan']
        .mean()
        .reset_index()
    )

    X = yearly[['Jaar']].values
    y = yearly['Doorlooptijd_Mediaan'].values

    model = LinearRegression().fit(X, y)
    residuals = y - model.predict(X)

    # 1a. Normality of residuals
    rec = shapiro_result(residuals, 'Regression residuals (yearly trend)')
    rec['analysis'] = 'Linear Regression'
    rec['deelvraag'] = 'Deelvraag 1'
    records.append(rec)
    logger.info(f"     Shapiro-Wilk residuals: W={rec['statistic']}, p={rec['p_value']}, met={rec['assumption_met']}")

    # 1b. Homoscedasticity — Breusch-Pagan
    X_const = sm.add_constant(X)
    try:
        bp_lm, bp_p, _, _ = het_breuschpagan(residuals, X_const)
        homoscedastic = bp_p >= 0.05
        records.append({
            'analysis': 'Linear Regression',
            'deelvraag': 'Deelvraag 1',
            'test': 'Breusch-Pagan',
            'variable': 'Regression residuals (yearly trend)',
            'statistic': round(bp_lm, 4),
            'p_value': fmt_p(bp_p),
            'assumption_met': homoscedastic,
            'interpretation': (
                "Homoscedastic residuals — OLS assumptions met"
                if homoscedastic
                else "Heteroscedastic residuals detected — consider robust standard errors"
            ),
        })
        logger.info(f"     Breusch-Pagan: LM={bp_lm:.4f}, p={fmt_p(bp_p)}, met={homoscedastic}")
    except Exception as e:
        logger.warning(f"     Breusch-Pagan failed: {e}")

    # 1c. Autocorrelation — Durbin-Watson (2.0 = no autocorrelation)
    dw_stat = durbin_watson(residuals)
    # DW: <1.5 = positive autocorrelation, >2.5 = negative, 1.5–2.5 = acceptable
    no_autocorr = 1.5 <= dw_stat <= 2.5
    records.append({
        'analysis': 'Linear Regression',
        'deelvraag': 'Deelvraag 1',
        'test': 'Durbin-Watson',
        'variable': 'Regression residuals (yearly trend)',
        'statistic': round(dw_stat, 4),
        'p_value': 'n/a',
        'assumption_met': no_autocorr,
        'interpretation': (
            "No autocorrelation in residuals (DW ≈ 2.0)"
            if no_autocorr
            else (
                "Positive autocorrelation detected (DW < 1.5) — expected in time series"
                if dw_stat < 1.5
                else "Negative autocorrelation detected (DW > 2.5)"
            )
        ),
    })
    logger.info(f"     Durbin-Watson: DW={dw_stat:.4f}, met={no_autocorr}")

    return records


def check_anova(df_doorloop: pd.DataFrame) -> list[dict]:
    """
    Analysis 2 — One-way ANOVA assumptions:
      - Normality per group (Shapiro-Wilk, first 5 groups logged)
      - Homogeneity of variances (Levene's)
    """
    logger.info("  [2] ANOVA assumptions")
    records = []

    df_clean = df_doorloop.dropna(subset=['Doorlooptijd_Mediaan', 'Regio_Naam'])
    groups_dict = {
        name: grp['Doorlooptijd_Mediaan'].values
        for name, grp in df_clean.groupby('Regio_Naam')
        if len(grp) >= 3
    }

    if len(groups_dict) < 2:
        logger.warning("     Not enough groups for ANOVA checks")
        return records

    # 2a. Normality per group — run all, summarise aggregate result
    normal_count = 0
    total_groups = len(groups_dict)
    non_normal_regions = []

    for region, values in groups_dict.items():
        rec = shapiro_result(values, region)
        if rec['assumption_met']:
            normal_count += 1
        else:
            non_normal_regions.append(region)

    pct_normal = round(100 * normal_count / total_groups, 1)
    assumption_met = pct_normal >= 80  # majority-rule threshold

    records.append({
        'analysis': 'One-way ANOVA',
        'deelvraag': 'Deelvraag 2',
        'test': 'Shapiro-Wilk (per group)',
        'variable': 'Doorlooptijd_Mediaan per Regio_Naam',
        'statistic': f"{normal_count}/{total_groups} groups normal",
        'p_value': 'multiple',
        'assumption_met': assumption_met,
        'interpretation': (
            f"{pct_normal}% of regions normally distributed — normality assumption broadly met"
            if assumption_met
            else (
                f"Only {pct_normal}% of regions normally distributed — "
                "consider Kruskal-Wallis as non-parametric alternative"
            )
        ),
    })
    logger.info(f"     Shapiro-Wilk per group: {normal_count}/{total_groups} normal ({pct_normal}%)")
    if non_normal_regions:
        logger.info(f"     Non-normal regions: {non_normal_regions[:5]}{'...' if len(non_normal_regions) > 5 else ''}")

    # 2b. Levene's test across all groups
    all_groups = list(groups_dict.values())
    stat, p = stats.levene(*all_groups, center='median')
    equal_var = p >= 0.05
    records.append({
        'analysis': 'One-way ANOVA',
        'deelvraag': 'Deelvraag 2',
        'test': "Levene's",
        'variable': 'Doorlooptijd_Mediaan across all regions',
        'statistic': round(stat, 4),
        'p_value': fmt_p(p),
        'assumption_met': equal_var,
        'interpretation': (
            "Equal variances across regions — standard ANOVA appropriate"
            if equal_var
            else "Unequal variances across regions — Welch's ANOVA recommended"
        ),
    })
    logger.info(f"     Levene's: W={stat:.4f}, p={fmt_p(p)}, met={equal_var}")

    return records


def check_ttest(df_doorloop: pd.DataFrame) -> list[dict]:
    """
    Analysis 4 — Independent t-test assumptions:
      - Normality per group (Shapiro-Wilk)
      - Equality of variances (Levene's)
    Note: equal_var=False (Welch's) is already used in analyze_statistics.py,
          so even if Levene fails the test is still valid.
    """
    logger.info("  [3/4] T-test assumptions")
    records = []

    df_filtered = df_doorloop[
        df_doorloop['Woningtype_Naam'] != 'Totaal'
    ].dropna(subset=['Doorlooptijd_Mediaan', 'Woningtype_Naam'])

    eengezins = df_filtered[
        df_filtered['Woningtype_Naam'].str.contains('Eengezins', case=False, na=False)
    ]['Doorlooptijd_Mediaan'].values

    meergezins = df_filtered[
        df_filtered['Woningtype_Naam'].str.contains('Meergezins', case=False, na=False)
    ]['Doorlooptijd_Mediaan'].values

    if len(eengezins) < 3 or len(meergezins) < 3:
        logger.warning("     Insufficient data for t-test assumption checks")
        return records

    for values, label in [(eengezins, 'Eengezinswoning'), (meergezins, 'Meergezinswoning')]:
        rec = shapiro_result(values, label)
        rec['analysis'] = 'Independent t-test'
        rec['deelvraag'] = 'Deelvraag 4'
        records.append(rec)
        logger.info(f"     Shapiro-Wilk {label}: W={rec['statistic']}, p={rec['p_value']}, met={rec['assumption_met']}")

    # Levene's
    rec = levene_result(eengezins, meergezins, 'Eengezinswoning', 'Meergezinswoning')
    rec['analysis'] = 'Independent t-test'
    rec['deelvraag'] = 'Deelvraag 4'
    if not rec['assumption_met']:
        rec['interpretation'] += " (Welch correction already applied in analysis)"
    records.append(rec)
    logger.info(f"     Levene's: W={rec['statistic']}, p={rec['p_value']}, met={rec['assumption_met']}")

    return records


def check_correlations(df_doorloop: pd.DataFrame, df_pijplijn: pd.DataFrame) -> list[dict]:
    """
    Analysis 5 — Pearson correlation assumptions:
      - Normality of each variable (Shapiro-Wilk)
      - Linearity noted as assumption (visual check recommended)
    """
    logger.info("  [5] Pearson correlation assumptions")
    records = []

    # Cross-dataset pair: Doorlooptijd_Mediaan ~ Bottleneck_2Jaar_Pct
    try:
        dl_agg = (
            df_doorloop.groupby(['Regio_Naam', 'Jaar'])['Doorlooptijd_Mediaan'].mean()
        )
        pj_agg = (
            df_pijplijn.groupby(['Regio_Naam', 'Jaar'])['Bottleneck_2Jaar_Pct'].mean()
        )
        merged = pd.concat([dl_agg, pj_agg], axis=1, join='inner').dropna()

        for col, label in [
            ('Doorlooptijd_Mediaan', 'Doorlooptijd_Mediaan (aggregated)'),
            ('Bottleneck_2Jaar_Pct', 'Bottleneck_2Jaar_Pct (aggregated)'),
        ]:
            rec = shapiro_result(merged[col].values, label)
            rec['analysis'] = 'Pearson Correlation'
            rec['deelvraag'] = 'Deelvraag 3/5'
            records.append(rec)
            logger.info(f"     Shapiro-Wilk {label}: W={rec['statistic']}, p={rec['p_value']}, met={rec['assumption_met']}")
    except Exception as e:
        logger.warning(f"     Cross-dataset correlation check skipped: {e}")

    # Within-pijplijn pairs
    for col in ['Bottleneck_2Jaar_Pct', 'Vergunning_Fase_Pct', 'Bouw_Fase_Pct']:
        if col not in df_pijplijn.columns:
            continue
        values = df_pijplijn[col].dropna().values
        rec = shapiro_result(values, f'{col} (pijplijn)')
        rec['analysis'] = 'Pearson Correlation'
        rec['deelvraag'] = 'Deelvraag 3/5'
        records.append(rec)
        logger.info(f"     Shapiro-Wilk {col}: W={rec['statistic']}, p={rec['p_value']}, met={rec['assumption_met']}")

    # Linearity note (visual check — cannot be automated without plots)
    records.append({
        'analysis': 'Pearson Correlation',
        'deelvraag': 'Deelvraag 3/5',
        'test': 'Linearity (visual)',
        'variable': 'All correlation pairs',
        'statistic': 'n/a',
        'p_value': 'n/a',
        'assumption_met': None,
        'interpretation': (
            "Linearity assumed — verify via scatterplots in Power BI / EDA step"
        ),
    })

    return records


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 70)
    logger.info("ASSUMPTION CHECKS — Pre-validation for analyze_statistics.py")
    logger.info("=" * 70)

    # Load processed data
    doorloop_path = PROCESSED_DIR / 'doorlooptijden_latest.csv'
    pijplijn_path = PROCESSED_DIR / 'woningen_pijplijn_latest.csv'

    if not doorloop_path.exists() or not pijplijn_path.exists():
        raise FileNotFoundError(
            "Processed datasets not found in data/processed/. "
            "Run extract + transform scripts first."
        )

    df_doorloop = pd.read_csv(doorloop_path)
    df_pijplijn = pd.read_csv(pijplijn_path)
    logger.info(f"Loaded: doorlooptijden {df_doorloop.shape}, pijplijn {df_pijplijn.shape}\n")

    all_records = []

    logger.info("Running checks...\n")
    all_records += check_regression(df_doorloop)
    all_records += check_anova(df_doorloop)
    all_records += check_ttest(df_doorloop)
    all_records += check_correlations(df_doorloop, df_pijplijn)

    # STL — no formal assumption checks
    all_records.append({
        'analysis': 'STL Decomposition',
        'deelvraag': 'Deelvraag 5',
        'test': 'None required',
        'variable': 'Doorlooptijd_Mediaan / Pijplijn_Totaal',
        'statistic': 'n/a',
        'p_value': 'n/a',
        'assumption_met': True,
        'interpretation': (
            "STL is a descriptive/exploratory technique — "
            "no distributional assumptions apply"
        ),
    })

    # Save
    out_df = pd.DataFrame(all_records)

    # Reorder columns
    col_order = [
        'analysis', 'deelvraag', 'test', 'variable',
        'statistic', 'p_value', 'assumption_met', 'interpretation'
    ]
    out_df = out_df[[c for c in col_order if c in out_df.columns]]

    out_path = RESULTS_DIR / '0_assumption_checks.csv'
    out_df.to_csv(out_path, index=False)

    # Summary
    met     = out_df['assumption_met'].eq(True).sum()
    not_met = out_df['assumption_met'].eq(False).sum()
    na_     = out_df['assumption_met'].isna().sum()

    logger.info("\n" + "=" * 70)
    logger.info("✓ ASSUMPTION CHECKS COMPLETE")
    logger.info(f"  ✓ Met:          {met}")
    logger.info(f"  ✗ Not met:      {not_met}")
    logger.info(f"  ~ N/A:          {na_}")
    logger.info(f"  Saved to:       {out_path.absolute()}")
    logger.info("=" * 70)

    # Print violations to console for quick review
    violations = out_df[out_df['assumption_met'] == False]
    if not violations.empty:
        logger.info("\n⚠ VIOLATED ASSUMPTIONS (review before reporting):")
        for _, row in violations.iterrows():
            logger.info(f"  [{row['analysis']}] {row['test']} — {row['interpretation']}")


if __name__ == "__main__":
    main()