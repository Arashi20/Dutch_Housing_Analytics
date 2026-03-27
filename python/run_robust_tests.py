"""
Robust (Non-Parametric) Validation Tests for Dutch Housing Crisis Research

Runs as a complement to analyze_statistics.py after assumption checks
(check_assumptions.py) revealed normality violations for:
  - Analysis 2: ANOVA      → Kruskal-Wallis + Dunn post-hoc
  - Analysis 4: T-test     → Mann-Whitney U
  - Analysis 5: Pearson    → Spearman rank correlation

Results are saved alongside the parametric outputs in results/ so both
can be compared and imported side-by-side in Power BI.

Outputs:
  results/2b_regional_kruskal.csv          – Kruskal-Wallis overall test
  results/2b_regional_kruskal_posthoc.csv  – Dunn post-hoc (significant pairs)
  results/4b_woningtype_mannwhitney.csv    – Mann-Whitney U
  results/5b_spearman_correlations.csv     – Spearman correlations
"""

import pandas as pd
import numpy as np
from scipy import stats
from pathlib import Path
import logging
import warnings

# ── Directory setup ───────────────────────────────────────────────────────────
Path('logs').mkdir(exist_ok=True)
Path('results').mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/robust_tests.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore')

PROCESSED_DIR = Path('data/processed')
RESULTS_DIR   = Path('results')


# ── Helpers ───────────────────────────────────────────────────────────────────

def format_pvalue(p: float) -> str:
    """Consistent p-value formatting (mirrors analyze_statistics.py)."""
    if p < 1e-16:
        return "<1e-16"
    elif p < 0.001:
        return f"{p:.4e}"
    else:
        return f"{p:.6f}"


def check_columns(df: pd.DataFrame, required: list, name: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {name}: {missing}")


# ── Test 1: Kruskal-Wallis + Dunn post-hoc (replaces ANOVA) ──────────────────

def run_kruskal_wallis(df_doorloop: pd.DataFrame):
    """
    Analysis 2b: Kruskal-Wallis H-test
    Non-parametric alternative to one-way ANOVA.
    Followed by Dunn's post-hoc test with Bonferroni correction.

    Outputs:
      results/2b_regional_kruskal.csv
      results/2b_regional_kruskal_posthoc.csv
    """
    logger.info("  Running Kruskal-Wallis: Doorlooptijd_Mediaan ~ Regio_Naam")

    check_columns(df_doorloop, ['Regio_Naam', 'Doorlooptijd_Mediaan'], 'doorlooptijden')

    df_clean = df_doorloop.dropna(subset=['Doorlooptijd_Mediaan', 'Regio_Naam'])

    groups_dict = {
        name: grp['Doorlooptijd_Mediaan'].values
        for name, grp in df_clean.groupby('Regio_Naam')
        if len(grp) >= 2
    }

    if len(groups_dict) < 2:
        raise ValueError("Need at least 2 regions for Kruskal-Wallis")

    h_stat, p_value = stats.kruskal(*groups_dict.values())
    significant = p_value < 0.05
    n_total = sum(len(v) for v in groups_dict.values())

    # Eta-squared approximation for Kruskal-Wallis effect size
    # η² = (H - k + 1) / (n - k)  where k = number of groups
    k = len(groups_dict)
    eta_squared = (h_stat - k + 1) / (n_total - k) if n_total > k else 0.0
    eta_squared = max(0.0, round(eta_squared, 4))

    logger.info(f"  H={h_stat:.4f}, p={format_pvalue(p_value)}, η²={eta_squared:.4f}")

    interpretation = (
        f"Significant regional differences detected (η²={eta_squared:.3f})"
        if significant
        else "No significant regional differences"
    )

    kw_df = pd.DataFrame([{
        'test': 'kruskal_wallis',
        'h_statistic': round(h_stat, 4),
        'p_value': format_pvalue(p_value),
        'significant': significant,
        'eta_squared': eta_squared,
        'n_groups': k,
        'n_total': n_total,
        'interpretation': interpretation,
        'parametric_equivalent': 'one_way_anova (results/2_regional_anova.csv)',
    }])

    kw_path = RESULTS_DIR / '2b_regional_kruskal.csv'
    kw_df.to_csv(kw_path, index=False)
    logger.info(f"  ✓ Saved: {kw_path}")

    # ── Dunn post-hoc with Bonferroni correction ──────────────────────────────
    # scipy has no built-in Dunn — implement manually using rank-sum z-scores
    logger.info("  Running Dunn post-hoc (Bonferroni correction)")

    region_names = list(groups_dict.keys())
    all_values = np.concatenate(list(groups_dict.values()))
    all_ranks  = stats.rankdata(all_values)

    # Build rank lookup per group
    pos = 0
    group_ranks = {}
    for name, values in groups_dict.items():
        n = len(values)
        group_ranks[name] = all_ranks[pos:pos + n]
        pos += n

    n_total_dunn = len(all_values)
    pairs = [
        (region_names[i], region_names[j])
        for i in range(len(region_names))
        for j in range(i + 1, len(region_names))
    ]
    n_comparisons = len(pairs)

    records = []
    for g1, g2 in pairs:
        r1 = group_ranks[g1]
        r2 = group_ranks[g2]
        n1, n2 = len(r1), len(r2)

        mean_r1 = r1.mean()
        mean_r2 = r2.mean()
        mean_diff = mean_r1 - mean_r2

        # Standard error under H0
        se = np.sqrt(
            (n_total_dunn * (n_total_dunn + 1) / 12)
            * (1 / n1 + 1 / n2)
        )
        if se == 0:
            continue

        z = mean_diff / se
        p_raw = 2 * (1 - stats.norm.cdf(abs(z)))

        # Bonferroni correction
        p_adj = min(p_raw * n_comparisons, 1.0)
        sig = p_adj < 0.05

        if sig:
            records.append({
                'group1': g1,
                'group2': g2,
                'mean_rank_diff': round(mean_diff, 4),
                'z_statistic': round(z, 4),
                'p_value_raw': format_pvalue(p_raw),
                'p_value_bonferroni': format_pvalue(p_adj),
                'significant': sig,
                'interpretation': (
                    f"{g1} significant hogere rangorde doorlooptijd"
                    if mean_diff > 0
                    else f"{g2} significant hogere rangorde doorlooptijd"
                ),
                'parametric_equivalent': 'tukey_hsd (results/2_regional_anova_posthoc.csv)',
            })

    posthoc_path = RESULTS_DIR / '2b_regional_kruskal_posthoc.csv'
    pd.DataFrame(records).to_csv(posthoc_path, index=False)
    logger.info(f"  ✓ Saved {len(records)} significant pairs (of {n_comparisons}): {posthoc_path}")


# ── Test 2: Mann-Whitney U (replaces t-test) ──────────────────────────────────

def run_mann_whitney(df_doorloop: pd.DataFrame):
    """
    Analysis 4b: Mann-Whitney U test
    Non-parametric alternative to independent samples t-test.

    Output: results/4b_woningtype_mannwhitney.csv
    """
    logger.info("  Running Mann-Whitney U: Doorlooptijd_Mediaan ~ Woningtype_Naam")

    check_columns(df_doorloop, ['Woningtype_Naam', 'Doorlooptijd_Mediaan'], 'doorlooptijden')

    df_filtered = df_doorloop[
        df_doorloop['Woningtype_Naam'] != 'Totaal'
    ].dropna(subset=['Doorlooptijd_Mediaan', 'Woningtype_Naam'])

    group_een = df_filtered[
        df_filtered['Woningtype_Naam'].str.contains('Eengezins', case=False, na=False)
    ]['Doorlooptijd_Mediaan'].values

    group_meer = df_filtered[
        df_filtered['Woningtype_Naam'].str.contains('Meergezins', case=False, na=False)
    ]['Doorlooptijd_Mediaan'].values

    if len(group_een) < 2 or len(group_meer) < 2:
        raise ValueError(
            f"Not enough samples. Eengezins n={len(group_een)}, Meergezins n={len(group_meer)}"
        )

    u_stat, p_value = stats.mannwhitneyu(group_een, group_meer, alternative='two-sided')
    significant = p_value < 0.05

    # Effect size r = Z / sqrt(N)
    n1, n2 = len(group_een), len(group_meer)
    n_total = n1 + n2
    # Z-approximation from U
    mean_u = n1 * n2 / 2
    std_u  = np.sqrt(n1 * n2 * (n1 + n2 + 1) / 12)
    z_score = (u_stat - mean_u) / std_u if std_u > 0 else 0.0
    r_effect = abs(z_score) / np.sqrt(n_total)

    abs_r = r_effect
    if abs_r >= 0.5:
        effect_label = "large effect size"
    elif abs_r >= 0.3:
        effect_label = "medium effect size"
    else:
        effect_label = "small effect size"

    median_een  = np.median(group_een)
    median_meer = np.median(group_meer)

    if significant:
        longer = "Meergezins" if median_meer > median_een else "Eengezins"
        interpretation = f"{longer} significant hogere mediaan doorlooptijd ({effect_label})"
    else:
        interpretation = f"Geen significant verschil in rangorde ({effect_label})"

    logger.info(
        f"  U={u_stat:.1f}, Z={z_score:.4f}, p={format_pvalue(p_value)}, "
        f"r={r_effect:.4f}, median_een={median_een:.2f}, median_meer={median_meer:.2f}"
    )

    result_df = pd.DataFrame([{
        'woningtype_1': 'Eengezinswoning',
        'woningtype_2': 'Meergezinswoning',
        'median_1': round(median_een, 4),
        'median_2': round(median_meer, 4),
        'n_1': n1,
        'n_2': n2,
        'u_statistic': round(u_stat, 1),
        'z_score': round(z_score, 4),
        'p_value': format_pvalue(p_value),
        'significant': significant,
        'effect_size_r': round(r_effect, 4),
        'interpretation': interpretation,
        'parametric_equivalent': 'welch_ttest (results/4_woningtype_ttest.csv)',
    }])

    out_path = RESULTS_DIR / '4b_woningtype_mannwhitney.csv'
    result_df.to_csv(out_path, index=False)
    logger.info(f"  ✓ Saved: {out_path}")


# ── Test 3: Spearman correlations (replaces Pearson) ─────────────────────────

def run_spearman_correlations(df_doorloop: pd.DataFrame, df_pijplijn: pd.DataFrame):
    """
    Analysis 5b: Spearman rank correlations
    Non-parametric alternative to Pearson for non-normal variables.

    Output: results/5b_spearman_correlations.csv
    """
    logger.info("  Computing Spearman correlations between key variables")

    records = []

    def _spearman_pair(series1, series2, label1, label2, interpretation_hint: str):
        combined = pd.DataFrame({'a': series1, 'b': series2}).dropna()
        n = len(combined)
        if n < 3:
            logger.warning(f"  Insufficient data for {label1} vs {label2} (n={n})")
            return None
        rho, p = stats.spearmanr(combined['a'], combined['b'])
        abs_rho = abs(rho)
        strength = 'Strong' if abs_rho >= 0.7 else ('Moderate' if abs_rho >= 0.4 else 'Weak')
        interp = (
            interpretation_hint
            if rho > 0 and p < 0.05
            else f"Zwakke of negatieve rangcorrelatie ({strength})"
        )
        return {
            'variable_1': label1,
            'variable_2': label2,
            'spearman_rho': round(rho, 4),
            'p_value': format_pvalue(p),
            'n_samples': n,
            'significant': p < 0.05,
            'strength': strength,
            'interpretation': interp,
            'parametric_equivalent': 'pearson (results/5_correlation_matrix.csv)',
        }

    # Pair 1: cross-dataset — Doorlooptijd_Mediaan vs Bottleneck_2Jaar_Pct
    try:
        check_columns(df_doorloop, ['Regio_Naam', 'Jaar', 'Doorlooptijd_Mediaan'], 'doorlooptijden')
        check_columns(df_pijplijn, ['Regio_Naam', 'Jaar', 'Bottleneck_2Jaar_Pct'], 'pijplijn')

        dl_agg = df_doorloop.groupby(['Regio_Naam', 'Jaar'])['Doorlooptijd_Mediaan'].mean()
        pj_agg = df_pijplijn.groupby(['Regio_Naam', 'Jaar'])['Bottleneck_2Jaar_Pct'].mean()
        merged = pd.concat([dl_agg, pj_agg], axis=1, join='inner').dropna()

        if len(merged) >= 3:
            rec = _spearman_pair(
                merged['Doorlooptijd_Mediaan'],
                merged['Bottleneck_2Jaar_Pct'],
                'Doorlooptijd_Mediaan',
                'Bottleneck_2Jaar_Pct',
                "Hogere bottleneck geassocieerd met langere doorlooptijd (rangcorrelatie)"
            )
            if rec:
                records.append(rec)
                logger.info(
                    f"  Doorlooptijd_Mediaan ~ Bottleneck_2Jaar_Pct: "
                    f"ρ={rec['spearman_rho']}, p={rec['p_value']}, n={rec['n_samples']}"
                )
        else:
            logger.warning(f"  Cross-dataset merge too small for Spearman (n={len(merged)})")

    except (ValueError, KeyError) as e:
        logger.warning(f"  Skipping cross-dataset Spearman: {e}")

    # Pairs within pijplijn dataset
    pj_pairs = [
        ('Bottleneck_2Jaar_Pct', 'Vergunning_Fase_Pct',
         "Bottlenecks sterk geassocieerd met vergunningsfase (rangcorrelatie)"),
        ('Bottleneck_2Jaar_Pct', 'Bouw_Fase_Pct',
         "Bottlenecks geassocieerd met bouwfase (rangcorrelatie)"),
    ]

    for v1, v2, hint in pj_pairs:
        if v1 not in df_pijplijn.columns or v2 not in df_pijplijn.columns:
            logger.warning(f"  Skipping {v1} ~ {v2}: column not found")
            continue
        rec = _spearman_pair(df_pijplijn[v1], df_pijplijn[v2], v1, v2, hint)
        if rec:
            records.append(rec)
            logger.info(
                f"  {v1} ~ {v2}: ρ={rec['spearman_rho']}, "
                f"p={rec['p_value']}, n={rec['n_samples']}"
            )

    if not records:
        raise ValueError("No Spearman correlations could be computed")

    out_path = RESULTS_DIR / '5b_spearman_correlations.csv'
    pd.DataFrame(records).to_csv(out_path, index=False)
    logger.info(f"  ✓ Saved {len(records)} Spearman pairs: {out_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 70)
    logger.info("ROBUST (NON-PARAMETRIC) VALIDATION TESTS")
    logger.info("Complement to analyze_statistics.py — run after check_assumptions.py")
    logger.info("=" * 70)

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

    logger.info("Test 1/3: Kruskal-Wallis + Dunn post-hoc (regional differences)")
    run_kruskal_wallis(df_doorloop)

    logger.info("\nTest 2/3: Mann-Whitney U (woningtype comparison)")
    run_mann_whitney(df_doorloop)

    logger.info("\nTest 3/3: Spearman rank correlations")
    run_spearman_correlations(df_doorloop, df_pijplijn)

    logger.info("\n" + "=" * 70)
    logger.info("✓ ROBUST TESTS COMPLETE")
    logger.info("New files in results/:")
    logger.info("  2b_regional_kruskal.csv")
    logger.info("  2b_regional_kruskal_posthoc.csv")
    logger.info("  4b_woningtype_mannwhitney.csv")
    logger.info("  5b_spearman_correlations.csv")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()