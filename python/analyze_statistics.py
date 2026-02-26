"""
Statistical Analysis for Dutch Housing Crisis Research

Implements Fase 2 analyses as defined in README:
- Temporal trend analysis (linear regression)
- Regional differences (ANOVA + post-hoc)
- Bottleneck quantification (descriptive stats)
- Woningtype comparison (t-test)
- Correlation analysis
- Seasonal decomposition (time series)

Outputs: results/*.csv files for Power BI import (Fase 3)
"""

import pandas as pd
import numpy as np
from scipy import stats
from sklearn.linear_model import LinearRegression
from statsmodels.stats.multicomp import pairwise_tukeyhsd
from statsmodels.tsa.seasonal import STL
from pathlib import Path
import logging
import warnings

# Create required directories before logging setup
Path('logs').mkdir(exist_ok=True)
Path('results').mkdir(exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/statistics.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Suppress warnings
warnings.filterwarnings('ignore')

# Directories
PROCESSED_DIR = Path('data/processed')
RESULTS_DIR = Path('results')
RESULTS_DIR.mkdir(exist_ok=True)


class HousingStatisticalAnalyzer:
    """Statistical analyzer for housing crisis research"""

    def __init__(self):
        self.df_doorloop = None
        self.df_pijplijn = None

    def load_data(self):
        """Load processed datasets"""
        logger.info("Loading processed datasets...")

        doorloop_file = PROCESSED_DIR / 'doorlooptijden_latest.csv'
        pijplijn_file = PROCESSED_DIR / 'woningen_pijplijn_latest.csv'

        if not doorloop_file.exists():
            raise FileNotFoundError(f"Doorlooptijden file not found: {doorloop_file}")
        if not pijplijn_file.exists():
            raise FileNotFoundError(f"Pijplijn file not found: {pijplijn_file}")

        self.df_doorloop = pd.read_csv(doorloop_file)
        self.df_pijplijn = pd.read_csv(pijplijn_file)

        logger.info(f"  ✓ Doorlooptijden: {len(self.df_doorloop):,} rows")
        logger.info(f"  ✓ Pijplijn: {len(self.df_pijplijn):,} rows")

    def _check_columns(self, df, required_cols, dataset_name):
        """Check that required columns exist in the dataframe"""
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(
                f"Missing columns in {dataset_name}: {missing}. "
                f"Available columns: {list(df.columns)}"
            )

    def analyze_temporal_trend(self):
        """
        Analysis 1: Temporal trend (Deelvraag 1)
        Linear regression of Doorlooptijd_Mediaan vs Jaar
        Output: results/1_temporal_regression.csv
        """
        logger.info("  Running linear regression: Doorlooptijd_Mediaan ~ Jaar")

        required = ['Jaar', 'Doorlooptijd_Mediaan']
        self._check_columns(self.df_doorloop, required, 'doorlooptijden')

        # Aggregate: mean Doorlooptijd_Mediaan per year
        yearly = (
            self.df_doorloop
            .dropna(subset=['Doorlooptijd_Mediaan'])
            .groupby('Jaar')['Doorlooptijd_Mediaan']
            .mean()
            .reset_index()
        )

        if len(yearly) < 2:
            raise ValueError("Not enough yearly observations for regression")

        X = yearly[['Jaar']].values
        y = yearly['Doorlooptijd_Mediaan'].values

        model = LinearRegression()
        model.fit(X, y)

        slope = model.coef_[0]
        intercept = model.intercept_
        r_squared = model.score(X, y)

        # Calculate p-value via scipy linregress
        sp_result = stats.linregress(yearly['Jaar'], yearly['Doorlooptijd_Mediaan'])
        p_value = sp_result.pvalue
        significant = p_value < 0.05

        logger.info(f"  Slope: {slope:.4f}, R²: {r_squared:.4f}, p={p_value:.4f}")

        # Build interpretation strings
        direction = "stijgt" if slope > 0 else "daalt"
        sig_text = "Highly significant (p<0.01)" if p_value < 0.01 else (
            "Significant (p<0.05)" if p_value < 0.05 else "Not significant (p>=0.05)"
        )

        records = [
            {
                'metric': 'slope',
                'value': round(slope, 4),
                'interpretation': f"Doorlooptijd {direction} {abs(slope):.2f} maanden per jaar"
            },
            {
                'metric': 'intercept',
                'value': round(intercept, 4),
                'interpretation': f"Baseline doorlooptijd in {int(yearly['Jaar'].min())}"
            },
            {
                'metric': 'r_squared',
                'value': round(r_squared, 4),
                'interpretation': f"{r_squared * 100:.1f}% variance verklaard door tijd"
            },
            {
                'metric': 'p_value',
                'value': round(p_value, 6),
                'interpretation': sig_text
            },
            {
                'metric': 'significant',
                'value': significant,
                'interpretation': "Trend is statistisch significant" if significant
                                 else "Trend is niet statistisch significant"
            },
        ]

        out_path = RESULTS_DIR / '1_temporal_regression.csv'
        pd.DataFrame(records).to_csv(out_path, index=False)
        logger.info(f"  ✓ Saved: {out_path}")

    def analyze_regional_anova(self):
        """
        Analysis 2: Regional differences (Deelvraag 2)
        One-way ANOVA + Tukey HSD post-hoc on Doorlooptijd_Mediaan by Regio_Naam
        Output: results/2_regional_anova.csv
                results/2_regional_anova_posthoc.csv
        """
        logger.info("  Running one-way ANOVA: Doorlooptijd_Mediaan ~ Regio_Naam")

        required = ['Regio_Naam', 'Doorlooptijd_Mediaan']
        self._check_columns(self.df_doorloop, required, 'doorlooptijden')

        df_clean = self.df_doorloop.dropna(subset=['Doorlooptijd_Mediaan', 'Regio_Naam'])

        # Build groups (list of arrays per region)
        groups = [
            grp['Doorlooptijd_Mediaan'].values
            for _, grp in df_clean.groupby('Regio_Naam')
            if len(grp) >= 2
        ]

        if len(groups) < 2:
            raise ValueError("Need at least 2 regions with sufficient data for ANOVA")

        f_stat, p_value = stats.f_oneway(*groups)
        significant = p_value < 0.05

        # Eta-squared effect size: SS_between / SS_total
        grand_mean = df_clean['Doorlooptijd_Mediaan'].mean()
        ss_total = ((df_clean['Doorlooptijd_Mediaan'] - grand_mean) ** 2).sum()
        group_means = df_clean.groupby('Regio_Naam')['Doorlooptijd_Mediaan'].mean()
        group_counts = df_clean.groupby('Regio_Naam')['Doorlooptijd_Mediaan'].count()
        ss_between = ((group_means - grand_mean) ** 2 * group_counts).sum()
        eta_squared = ss_between / ss_total if ss_total > 0 else 0.0

        logger.info(f"  F={f_stat:.4f}, p={p_value:.6f}, eta2={eta_squared:.4f}")

        interpretation = (
            f"Significant regional differences ({eta_squared * 100:.1f}% variance explained)"
            if significant
            else "No significant regional differences"
        )

        anova_df = pd.DataFrame([{
            'test': 'one_way_anova',
            'f_statistic': round(f_stat, 4),
            'p_value': round(p_value, 6),
            'significant': significant,
            'eta_squared': round(eta_squared, 4),
            'interpretation': interpretation,
        }])
        anova_path = RESULTS_DIR / '2_regional_anova.csv'
        anova_df.to_csv(anova_path, index=False)
        logger.info(f"  ✓ Saved: {anova_path}")

        # Tukey HSD post-hoc
        logger.info("  Running Tukey HSD post-hoc test")
        tukey = pairwise_tukeyhsd(
            endog=df_clean['Doorlooptijd_Mediaan'],
            groups=df_clean['Regio_Naam'],
            alpha=0.05
        )

        tukey_df = pd.DataFrame(
            data=tukey.summary().data[1:],
            columns=tukey.summary().data[0]
        )
        tukey_df = tukey_df.rename(columns={
            'meandiff': 'mean_diff',
            'p-adj': 'p_adj',
            'reject': 'significant',
        })

        # Filter to significant pairs only
        tukey_sig = tukey_df[tukey_df['significant']].copy()

        tukey_sig['interpretation'] = tukey_sig.apply(
            lambda r: (
                f"{r['group1']} {abs(float(r['mean_diff'])):.1f} maanden langer"
                if float(r['mean_diff']) > 0
                else f"{r['group2']} {abs(float(r['mean_diff'])):.1f} maanden langer"
            ),
            axis=1
        )

        posthoc_path = RESULTS_DIR / '2_regional_anova_posthoc.csv'
        tukey_sig.to_csv(posthoc_path, index=False)
        logger.info(f"  ✓ Saved {len(tukey_sig)} significant pairs: {posthoc_path}")

    def quantify_bottlenecks(self):
        """
        Analysis 3: Bottleneck quantification (Deelvraag 3)
        Descriptive statistics + ranking by Regio_Naam
        Output: results/3_bottleneck_summary.csv
                results/3_bottleneck_top10_crisis.csv
        """
        logger.info("  Calculating bottleneck statistics per region")

        required = [
            'Regio_Naam', 'Bottleneck_2Jaar_Pct', 'Bottleneck_5Jaar_Pct',
            'Vergunning_Fase_Pct', 'Bouw_Fase_Pct', 'Crisis_Regio'
        ]
        self._check_columns(self.df_pijplijn, required, 'woningen_pijplijn')

        # Group by region and compute averages
        summary = (
            self.df_pijplijn
            .groupby('Regio_Naam')
            .agg(
                Bottleneck_2Jaar_Pct_Avg=('Bottleneck_2Jaar_Pct', 'mean'),
                Bottleneck_5Jaar_Pct_Avg=('Bottleneck_5Jaar_Pct', 'mean'),
                Vergunning_Fase_Pct_Avg=('Vergunning_Fase_Pct', 'mean'),
                Bouw_Fase_Pct_Avg=('Bouw_Fase_Pct', 'mean'),
                Crisis_Regio=('Crisis_Regio', 'max'),
            )
            .reset_index()
        )

        # Round percentages
        pct_cols = [
            'Bottleneck_2Jaar_Pct_Avg', 'Bottleneck_5Jaar_Pct_Avg',
            'Vergunning_Fase_Pct_Avg', 'Bouw_Fase_Pct_Avg'
        ]
        summary[pct_cols] = summary[pct_cols].round(2)

        # Rank by Bottleneck_2Jaar_Pct_Avg (descending)
        summary = summary.sort_values('Bottleneck_2Jaar_Pct_Avg', ascending=False)
        summary['Rank'] = range(1, len(summary) + 1)

        summary_path = RESULTS_DIR / '3_bottleneck_summary.csv'
        summary.to_csv(summary_path, index=False)
        logger.info(f"  ✓ Saved {len(summary)} regions: {summary_path}")

        # Top 10 crisis gemeentes
        top10 = summary.head(10).copy()
        top10 = top10.rename(columns={'Vergunning_Fase_Pct_Avg': 'Vergunning_Bottleneck_Pct_Avg'})

        def _top10_interpretation(row):
            rank = int(row['Rank'])
            pct = row['Bottleneck_2Jaar_Pct_Avg']
            if rank == 1:
                return (
                    f"Highest bottleneck - 1 in {max(1, round(100/max(pct, 0.01)))} "
                    "projecten vast >2 jaar"
                )
            elif rank <= 3:
                return "Top 3 bottleneck - vergunning primary issue"
            else:
                return f"Rank {rank} - significant bottleneck ({pct:.1f}% vast >2 jaar)"

        top10['Interpretation'] = top10.apply(_top10_interpretation, axis=1)

        top10_cols = [
            'Rank', 'Regio_Naam', 'Bottleneck_2Jaar_Pct_Avg',
            'Bottleneck_5Jaar_Pct_Avg', 'Vergunning_Bottleneck_Pct_Avg', 'Interpretation'
        ]
        top10_path = RESULTS_DIR / '3_bottleneck_top10_crisis.csv'
        top10[top10_cols].to_csv(top10_path, index=False)
        logger.info(f"  ✓ Saved top-10 crisis regions: {top10_path}")

    def compare_woningtype(self):
        """
        Analysis 4: Woningtype comparison (Deelvraag 4)
        Independent samples t-test: Eengezinswoning vs Meergezinswoning
        Output: results/4_woningtype_ttest.csv
        """
        logger.info("  Running t-test: Doorlooptijd_Mediaan ~ Woningtype_Naam")

        required = ['Woningtype_Naam', 'Doorlooptijd_Mediaan']
        self._check_columns(self.df_doorloop, required, 'doorlooptijden')

        df_filtered = self.df_doorloop[
            self.df_doorloop['Woningtype_Naam'] != 'Totaal'
        ].dropna(subset=['Doorlooptijd_Mediaan', 'Woningtype_Naam'])

        unique_types = df_filtered['Woningtype_Naam'].unique()
        logger.info(f"  Housing types (excl. Totaal): {unique_types}")

        # Identify Eengezins and Meergezins groups
        eengezins_mask = df_filtered['Woningtype_Naam'].str.contains(
            'Eengezins', case=False, na=False
        )
        meergezins_mask = df_filtered['Woningtype_Naam'].str.contains(
            'Meergezins', case=False, na=False
        )

        group_een = df_filtered[eengezins_mask]['Doorlooptijd_Mediaan'].values
        group_meer = df_filtered[meergezins_mask]['Doorlooptijd_Mediaan'].values

        if len(group_een) < 2 or len(group_meer) < 2:
            raise ValueError(
                "Not enough samples for t-test. "
                f"Eengezins n={len(group_een)}, Meergezins n={len(group_meer)}"
            )

        t_stat, p_value = stats.ttest_ind(group_een, group_meer, equal_var=False)
        significant = p_value < 0.05

        mean_een = group_een.mean()
        mean_meer = group_meer.mean()
        mean_diff = mean_een - mean_meer

        # Cohen's d effect size
        pooled_std = np.sqrt(
            (group_een.std(ddof=1) ** 2 + group_meer.std(ddof=1) ** 2) / 2
        )
        cohens_d = mean_diff / pooled_std if pooled_std > 0 else 0.0

        abs_d = abs(cohens_d)
        if abs_d >= 0.8:
            effect_label = "large effect size"
        elif abs_d >= 0.5:
            effect_label = "medium effect size"
        else:
            effect_label = "small effect size"

        if significant:
            longer = "Meergezins" if mean_meer > mean_een else "Eengezins"
            interpretation = f"{longer} significant langer ({effect_label})"
        else:
            interpretation = f"Geen significant verschil ({effect_label})"

        logger.info(
            f"  t={t_stat:.4f}, p={p_value:.6f}, "
            f"mean_een={mean_een:.2f}, mean_meer={mean_meer:.2f}, d={cohens_d:.4f}"
        )

        result_df = pd.DataFrame([{
            'woningtype_1': 'Eengezinswoning',
            'woningtype_2': 'Meergezinswoning',
            'mean_1': round(mean_een, 4),
            'mean_2': round(mean_meer, 4),
            'mean_diff': round(mean_diff, 4),
            't_statistic': round(t_stat, 4),
            'p_value': round(p_value, 6),
            'significant': significant,
            'cohens_d': round(cohens_d, 4),
            'interpretation': interpretation,
        }])

        out_path = RESULTS_DIR / '4_woningtype_ttest.csv'
        result_df.to_csv(out_path, index=False)
        logger.info(f"  ✓ Saved: {out_path}")

    def analyze_correlations(self):
        """
        Analysis 5: Correlation analysis
        Pearson correlations between key variables
        Output: results/5_correlation_matrix.csv
        """
        logger.info("  Computing Pearson correlations between key variables")

        records = []

        def _pearson_pair(series1, series2, label1, label2):
            """Compute Pearson r for two aligned series, drop NaNs pairwise."""
            combined = pd.DataFrame({'a': series1, 'b': series2}).dropna()
            n = len(combined)
            if n < 3:
                logger.warning(f"  Insufficient data for {label1} vs {label2} (n={n})")
                return None
            r, p = stats.pearsonr(combined['a'], combined['b'])
            abs_r = abs(r)
            if abs_r >= 0.7:
                strength = 'Strong'
            elif abs_r >= 0.4:
                strength = 'Moderate'
            else:
                strength = 'Weak'
            return {
                'variable_1': label1,
                'variable_2': label2,
                'correlation': round(r, 4),
                'p_value': round(p, 6),
                'n_samples': n,
                'significant': p < 0.05,
                'strength': strength,
            }

        # Pair 1: Doorlooptijd_Mediaan vs Bottleneck_2Jaar_Pct
        # Aggregate both datasets to Regio_Naam + Jaar level for cross-dataset join
        req_dl = ['Regio_Naam', 'Jaar', 'Doorlooptijd_Mediaan']
        req_pj = ['Regio_Naam', 'Jaar', 'Bottleneck_2Jaar_Pct']
        try:
            self._check_columns(self.df_doorloop, req_dl, 'doorlooptijden')
            self._check_columns(self.df_pijplijn, req_pj, 'woningen_pijplijn')

            dl_agg = (
                self.df_doorloop.groupby(['Regio_Naam', 'Jaar'])['Doorlooptijd_Mediaan']
                .mean()
            )
            pj_agg = (
                self.df_pijplijn.groupby(['Regio_Naam', 'Jaar'])['Bottleneck_2Jaar_Pct']
                .mean()
            )
            merged = pd.concat([dl_agg, pj_agg], axis=1, join='inner').dropna()

            if len(merged) >= 3:
                r, p = stats.pearsonr(
                    merged['Doorlooptijd_Mediaan'], merged['Bottleneck_2Jaar_Pct']
                )
                abs_r = abs(r)
                strength = 'Strong' if abs_r >= 0.7 else ('Moderate' if abs_r >= 0.4 else 'Weak')
                records.append({
                    'variable_1': 'Doorlooptijd_Mediaan',
                    'variable_2': 'Bottleneck_2Jaar_Pct',
                    'correlation': round(r, 4),
                    'p_value': round(p, 6),
                    'n_samples': len(merged),
                    'significant': p < 0.05,
                    'strength': strength,
                    'interpretation': (
                        "Hogere bottleneck geassocieerd met langere doorlooptijd"
                        if r > 0
                        else "Hogere bottleneck geassocieerd met kortere doorlooptijd"
                    ),
                })
                logger.info(
                    f"  Doorlooptijd_Mediaan ~ Bottleneck_2Jaar_Pct: "
                    f"r={r:.4f}, p={p:.6f}, n={len(merged)}"
                )
        except (ValueError, KeyError) as e:
            logger.warning(f"  Skipping cross-dataset correlation: {e}")

        # Pairs within pijplijn dataset
        pj_pairs = [
            ('Bottleneck_2Jaar_Pct', 'Vergunning_Fase_Pct',
             "Bottlenecks vooral in vergunningsfase"),
            ('Bottleneck_2Jaar_Pct', 'Bouw_Fase_Pct',
             "Relatie tussen bottlenecks en bouwfase"),
        ]
        for v1, v2, interp_hint in pj_pairs:
            if v1 not in self.df_pijplijn.columns or v2 not in self.df_pijplijn.columns:
                logger.warning(f"  Skipping {v1} ~ {v2}: column not found")
                continue
            rec = _pearson_pair(
                self.df_pijplijn[v1], self.df_pijplijn[v2], v1, v2
            )
            if rec:
                if rec['correlation'] > 0 and rec['significant']:
                    rec['interpretation'] = interp_hint
                else:
                    rec['interpretation'] = (
                        f"Zwakke of negatieve correlatie ({rec['strength']})"
                    )
                records.append(rec)
                logger.info(
                    f"  {v1} ~ {v2}: r={rec['correlation']}, "
                    f"p={rec['p_value']}, n={rec['n_samples']}"
                )

        if not records:
            raise ValueError("No correlations could be computed")

        out_path = RESULTS_DIR / '5_correlation_matrix.csv'
        pd.DataFrame(records).to_csv(out_path, index=False)
        logger.info(f"  ✓ Saved {len(records)} correlation pairs: {out_path}")

    def decompose_seasonal(self):
        """
        Analysis 6: Seasonal decomposition (Deelvraag 5)
        STL decomposition for doorlooptijden (quarterly) and pijplijn (monthly)
        Output: results/6_seasonal_decomposition_doorlooptijd.csv
                results/6_seasonal_decomposition_pijplijn.csv
        """
        logger.info("  Running STL seasonal decomposition")

        # Dataset 1: Quarterly doorlooptijden
        req_dl = ['Jaar', 'Kwartaal', 'Periode_Naam', 'Doorlooptijd_Mediaan']
        self._check_columns(self.df_doorloop, req_dl, 'doorlooptijden')

        dl_ts = (
            self.df_doorloop
            .dropna(subset=['Doorlooptijd_Mediaan'])
            .groupby(['Jaar', 'Kwartaal'])
            .agg(
                Periode_Naam=('Periode_Naam', 'first'),
                Observed=('Doorlooptijd_Mediaan', 'mean')
            )
            .reset_index()
            .sort_values(['Jaar', 'Kwartaal'])
        )

        # Forward-fill any gaps
        dl_ts['Observed'] = dl_ts['Observed'].ffill()

        min_periods_quarterly = 2 * 4 + 1  # 2 * period + 1 for STL
        if len(dl_ts) >= min_periods_quarterly:
            stl = STL(dl_ts['Observed'], period=4, robust=True)
            result = stl.fit()
            dl_ts['Trend'] = result.trend.round(4)
            dl_ts['Seasonal'] = result.seasonal.round(4)
            dl_ts['Residual'] = result.resid.round(4)
            dl_ts['Observed'] = dl_ts['Observed'].round(4)
            logger.info(f"  STL decomposition: {len(dl_ts)} quarterly observations")
        else:
            logger.warning(
                f"  Insufficient data for quarterly STL "
                f"(n={len(dl_ts)}, need {min_periods_quarterly}). "
                "Saving observed values only."
            )
            dl_ts['Trend'] = np.nan
            dl_ts['Seasonal'] = np.nan
            dl_ts['Residual'] = np.nan

        dl_out_cols = [
            'Jaar', 'Kwartaal', 'Periode_Naam', 'Observed', 'Trend', 'Seasonal', 'Residual'
        ]
        dl_path = RESULTS_DIR / '6_seasonal_decomposition_doorlooptijd.csv'
        dl_ts[dl_out_cols].to_csv(dl_path, index=False)
        logger.info(f"  ✓ Saved doorlooptijd decomposition: {dl_path}")

        # Dataset 2: Monthly pijplijn
        req_pj = ['Jaar', 'Maand', 'Periode_Naam', 'Pijplijn_Totaal']
        self._check_columns(self.df_pijplijn, req_pj, 'woningen_pijplijn')

        pj_ts = (
            self.df_pijplijn
            .dropna(subset=['Pijplijn_Totaal'])
            .groupby(['Jaar', 'Maand'])
            .agg(
                Periode_Naam=('Periode_Naam', 'first'),
                Observed=('Pijplijn_Totaal', 'mean')
            )
            .reset_index()
            .sort_values(['Jaar', 'Maand'])
        )

        # Forward-fill any gaps
        pj_ts['Observed'] = pj_ts['Observed'].ffill()

        min_periods_monthly = 2 * 12 + 1  # 2 * period + 1 for STL
        if len(pj_ts) >= min_periods_monthly:
            stl = STL(pj_ts['Observed'], period=12, robust=True)
            result = stl.fit()
            pj_ts['Trend'] = result.trend.round(2)
            pj_ts['Seasonal'] = result.seasonal.round(2)
            pj_ts['Residual'] = result.resid.round(2)
            pj_ts['Observed'] = pj_ts['Observed'].round(2)
            logger.info(f"  STL decomposition: {len(pj_ts)} monthly observations")
        else:
            logger.warning(
                f"  Insufficient data for monthly STL "
                f"(n={len(pj_ts)}, need {min_periods_monthly}). "
                "Saving observed values only."
            )
            pj_ts['Trend'] = np.nan
            pj_ts['Seasonal'] = np.nan
            pj_ts['Residual'] = np.nan

        pj_out_cols = [
            'Jaar', 'Maand', 'Periode_Naam', 'Observed', 'Trend', 'Seasonal', 'Residual'
        ]
        pj_path = RESULTS_DIR / '6_seasonal_decomposition_pijplijn.csv'
        pj_ts[pj_out_cols].to_csv(pj_path, index=False)
        logger.info(f"  ✓ Saved pijplijn decomposition: {pj_path}")

    def run_all_analyses(self):
        """Run complete statistical analysis workflow"""
        logger.info("=" * 70)
        logger.info("STARTING FASE 2: STATISTICAL ANALYSIS")
        logger.info("=" * 70)

        try:
            self.load_data()

            logger.info("\nAnalysis 1/6: Temporal Trend Analysis")
            self.analyze_temporal_trend()

            logger.info("\nAnalysis 2/6: Regional ANOVA")
            self.analyze_regional_anova()

            logger.info("\nAnalysis 3/6: Bottleneck Quantification")
            self.quantify_bottlenecks()

            logger.info("\nAnalysis 4/6: Woningtype T-test")
            self.compare_woningtype()

            logger.info("\nAnalysis 5/6: Correlation Analysis")
            self.analyze_correlations()

            logger.info("\nAnalysis 6/6: Seasonal Decomposition")
            self.decompose_seasonal()

            logger.info("\n" + "=" * 70)
            logger.info("✓ FASE 2 COMPLETE!")
            logger.info(f"✓ Results saved to: {RESULTS_DIR.absolute()}")
            logger.info("=" * 70)

        except Exception as e:
            logger.error(f"Analysis failed: {str(e)}", exc_info=True)
            raise


def main():
    """Main analysis workflow"""
    analyzer = HousingStatisticalAnalyzer()
    analyzer.run_all_analyses()


if __name__ == "__main__":
    main()
