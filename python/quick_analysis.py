"""
Quick Exploratory Analysis
===========================
Visual exploration of Dutch Housing Analytics data (2015-2025, before adding dataset 2).

Purpose:
- Quick insights before formal statistical analysis
- Visualize temporal trends (Deelvraag 1)
- Compare regions (Deelvraag 2)
- Compare housing types (Deelvraag 4)

outputs:
- images in visualizations/ folder 

Author: Arashi20
Date: 2026-02-23
Project: Dutch Housing Analytics - Rijksoverheid Portfolio
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path

from config import (
    PROCESSED_DATA_DIR,
    PROJECT_ROOT,
    get_logger
)

# Initialize logger
logger = get_logger(__name__)

# Create visualizations directory if it doesn't exist
VIZ_DIR = PROJECT_ROOT / 'visualizations'
VIZ_DIR.mkdir(exist_ok=True)

# Set visualization style (consistent across all charts)
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 7)
plt.rcParams['font.size'] = 11


def load_data() -> pd.DataFrame:
    """
    Load transformed doorlooptijden data.
    
    Returns:
        DataFrame with full 2015-2025 dataset
    """
    logger.info("Loading transformed data")
    
    data_file = PROCESSED_DATA_DIR / 'doorlooptijden_latest.csv'
    
    if not data_file.exists():
        raise FileNotFoundError(
            f"Data file not found: {data_file}\n"
            f"Please run transformation first:\n"
            f"  python python/transform_housing_data.py"
        )
    
    df = pd.read_csv(data_file)
    
    logger.info(f"Loaded {len(df):,} rows")
    logger.info(f"Year range: {df['Jaar'].min()}-{df['Jaar'].max()}")
    
    return df


def plot_temporal_trend(df: pd.DataFrame):
    """
    Plot temporal trend of doorlooptijd (2015-2025).
    
    Answers: Deelvraag 1 - "Hoe is doorlooptijd veranderd sinds 2015?"
    
    Args:
        df: Full dataset
    """
    logger.info("Creating temporal trend chart")
    
    # Filter: Nederland, Totaal (national aggregate)
    nl_total = df[
        (df['Regio_Naam'] == 'Nederland') & 
        (df['Woningtype_Naam'] == 'Totaal')
    ].copy()
    
    # Sort by time
    nl_total = nl_total.sort_values(['Jaar', 'Kwartaal'])
    
    # Create continuous time variable
    nl_total['Time'] = nl_total['Jaar'] + (nl_total['Kwartaal']-1)/4
    
    # Create figure
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # Plot trend
    ax.plot(nl_total['Time'], nl_total['Doorlooptijd_Mediaan'], 
            marker='o', linewidth=2.5, markersize=5, 
            color='#E17000', label='Mediaan Doorlooptijd')
    
    # Add reference lines
    ax.axhline(y=18, color='green', linestyle='--', linewidth=1.5, 
               alpha=0.7, label='Acceptabel: 18 maanden')
    ax.axhline(y=24, color='red', linestyle='--', linewidth=1.5, 
               alpha=0.7, label='Problematisch: 24+ maanden')
    
    # Highlight COVID-19 period
    ax.axvspan(2020, 2021.75, alpha=0.15, color='gray', label='COVID-19 Periode')
    
    # Formatting
    ax.set_title('Doorlooptijd Nieuwbouw Nederland 2015-2025\n'
                 'Van vergunningverlening tot oplevering',
                 fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Jaar', fontsize=13, fontweight='bold')
    ax.set_ylabel('Mediaan Doorlooptijd (maanden)', fontsize=13, fontweight='bold')
    ax.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)
    ax.legend(loc='upper left', fontsize=11, framealpha=0.9)
    
    # Add annotations
    start_val = nl_total.iloc[0]['Doorlooptijd_Mediaan']
    end_val = nl_total.iloc[-1]['Doorlooptijd_Mediaan']
    change = end_val - start_val
    change_pct = (change / start_val) * 100
    
    ax.annotate(f'2015 Q1: {start_val:.1f} mnd',
                xy=(nl_total.iloc[0]['Time'], start_val),
                xytext=(10, 10), textcoords='offset points',
                fontsize=10, color='darkgreen',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.8))
    
    ax.annotate(f'2025 Q4: {end_val:.1f} mnd\n({change:+.1f} mnd, {change_pct:+.1f}%)',
                xy=(nl_total.iloc[-1]['Time'], end_val),
                xytext=(-80, -30), textcoords='offset points',
                fontsize=10, color='darkred',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.8),
                arrowprops=dict(arrowstyle='->', color='darkred', lw=1.5))
    
    plt.tight_layout()
    
    # Save
    output_file = VIZ_DIR / 'trend_doorlooptijd_2015_2025.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    logger.info(f"✓ Saved: {output_file.name}")
    
    plt.show()
    
    # Print summary
    print("\n" + "="*60)
    print("TEMPORAL TREND ANALYSIS")
    print("="*60)
    print(f"Start (2015 Q1): {start_val:.1f} maanden")
    print(f"End (2025 Q4):   {end_val:.1f} maanden")
    print(f"Change:          {change:+.1f} maanden ({change_pct:+.1f}%)")
    
    if change > 0:
        print(f"\n⚠️  VERSLECHTERING: Doorlooptijd is {change:.1f} maanden LANGER geworden!")
    else:
        print(f"\n✓ VERBETERING: Doorlooptijd is {abs(change):.1f} maanden KORTER geworden!")
    
    print("="*60 + "\n")


def plot_regional_comparison(df: pd.DataFrame):
    """
    Plot regional comparison of doorlooptijd.
    
    Answers: Deelvraag 2 - "Verschillen tussen regio's?"
    
    Args:
        df: Full dataset
    """
    logger.info("Creating regional comparison chart")
    
    # Filter: Totaal woningtype, exclude Nederland (aggregate)
    regional = df[
        (df['Woningtype_Naam'] == 'Totaal') &
        (df['Regio_Naam'] != 'Nederland')
    ].copy()
    
    # Calculate average doorlooptijd per region
    regional_avg = regional.groupby('Regio_Naam')['Doorlooptijd_Mediaan'].mean().sort_values(ascending=False)
    
    # Top 10 slowest regions
    top10 = regional_avg.head(10)
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Create bar chart with color gradient (red = slow, yellow = faster)
    colors = plt.cm.RdYlGn_r(np.linspace(0.2, 0.8, len(top10)))
    bars = ax.barh(range(len(top10)), top10.values, color=colors)
    
    # Formatting
    ax.set_yticks(range(len(top10)))
    ax.set_yticklabels(top10.index)
    ax.set_xlabel('Gemiddelde Mediaan Doorlooptijd (maanden)', fontsize=13, fontweight='bold')
    ax.set_title('Top 10 Langzaamste Regio\'s voor Nieuwbouw (2015-2025)',
                 fontsize=16, fontweight='bold', pad=20)
    ax.grid(axis='x', alpha=0.3)
    
    # Add value labels on bars
    for i, (bar, val) in enumerate(zip(bars, top10.values)):
        ax.text(val + 0.3, i, f'{val:.1f}', 
                va='center', fontsize=10, fontweight='bold')
    
    # Add reference line (national average)
    national_avg = df[
        (df['Regio_Naam'] == 'Nederland') &
        (df['Woningtype_Naam'] == 'Totaal')
    ]['Doorlooptijd_Mediaan'].mean()
    
    ax.axvline(x=national_avg, color='blue', linestyle='--', linewidth=2,
               alpha=0.7, label=f'Landelijk gemiddelde: {national_avg:.1f} mnd')
    
    ax.legend(loc='lower right', fontsize=11)
    
    plt.tight_layout()
    
    # Save
    output_file = VIZ_DIR / 'regional_comparison_top10.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    logger.info(f"✓ Saved: {output_file.name}")
    
    plt.show()
    
    # Print summary
    print("\n" + "="*60)
    print("REGIONAL COMPARISON (Top 10 Slowest)")
    print("="*60)
    for i, (region, val) in enumerate(top10.items(), 1):
        diff = val - national_avg
        print(f"{i:2d}. {region:30s} {val:5.1f} mnd ({diff:+.1f} vs landelijk)")
    print("="*60 + "\n")


def plot_housing_type_comparison(df: pd.DataFrame):
    """
    Plot comparison between housing types.
    
    Answers: Deelvraag 4 - "Verschilt doorlooptijd tussen woningtypen?"
    
    Args:
        df: Full dataset
    """
    logger.info("Creating housing type comparison chart")
    
    # Filter: Nederland, exclude Totaal
    housing_types = df[
        (df['Regio_Naam'] == 'Nederland') &
        (df['Woningtype_Naam'] != 'Totaal')
    ].copy()
    
    # Create figure with subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # ────────────────────────────────────────────────────────────
    # Chart 1: Box plot (distribution)
    # ────────────────────────────────────────────────────────────
    sns.boxplot(data=housing_types, x='Woningtype_Naam', y='Doorlooptijd_Mediaan',
                palette=['#2ecc71', '#3498db'], ax=ax1)
    
    ax1.set_title('Verdeling Doorlooptijd per Woningtype',
                  fontsize=14, fontweight='bold')
    ax1.set_xlabel('Woningtype', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Mediaan Doorlooptijd (maanden)', fontsize=12, fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)
    
    # ────────────────────────────────────────────────────────────
    # Chart 2: Temporal trend by housing type
    # ────────────────────────────────────────────────────────────
    for woningtype in housing_types['Woningtype_Naam'].unique():
        subset = housing_types[housing_types['Woningtype_Naam'] == woningtype].copy()
        subset = subset.sort_values(['Jaar', 'Kwartaal'])
        subset['Time'] = subset['Jaar'] + (subset['Kwartaal']-1)/4
        
        color = '#2ecc71' if woningtype == 'Eengezinswoning' else '#3498db'
        ax2.plot(subset['Time'], subset['Doorlooptijd_Mediaan'],
                marker='o', linewidth=2, markersize=4, label=woningtype, color=color)
    
    ax2.set_title('Trend per Woningtype (2015-2025)',
                  fontsize=14, fontweight='bold')
    ax2.set_xlabel('Jaar', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Mediaan Doorlooptijd (maanden)', fontsize=12, fontweight='bold')
    ax2.legend(loc='upper left', fontsize=11)
    ax2.grid(alpha=0.3)
    
    plt.tight_layout()
    
    # Save
    output_file = VIZ_DIR / 'housing_type_comparison.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    logger.info(f"✓ Saved: {output_file.name}")
    
    plt.show()
    
    # Print summary
    print("\n" + "="*60)
    print("HOUSING TYPE COMPARISON")
    print("="*60)
    
    for woningtype in housing_types['Woningtype_Naam'].unique():
        subset = housing_types[housing_types['Woningtype_Naam'] == woningtype]
        mean_val = subset['Doorlooptijd_Mediaan'].mean()
        median_val = subset['Doorlooptijd_Mediaan'].median()
        std_val = subset['Doorlooptijd_Mediaan'].std()
        
        print(f"\n{woningtype}:")
        print(f"  Gemiddelde: {mean_val:.1f} maanden")
        print(f"  Mediaan:    {median_val:.1f} maanden")
        print(f"  Std Dev:    {std_val:.1f} maanden")
    
    # Calculate difference
    eengezins_avg = housing_types[housing_types['Woningtype_Naam'] == 'Eengezinswoning']['Doorlooptijd_Mediaan'].mean()
    meergezins_avg = housing_types[housing_types['Woningtype_Naam'] == 'Meergezinswoning']['Doorlooptijd_Mediaan'].mean()
    diff = meergezins_avg - eengezins_avg
    diff_pct = (diff / eengezins_avg) * 100
    
    print(f"\nVerschil:")
    print(f"  Meergezins vs Eengezins: {diff:+.1f} maanden ({diff_pct:+.1f}%)")
    
    if diff > 0:
        print(f"  → Meergezinswoningen duren {diff:.1f} maanden LANGER")
    else:
        print(f"  → Eengezinswoningen duren {abs(diff):.1f} maanden LANGER")
    
    print("="*60 + "\n")

def plot_bottleneck_by_region(df: pd.DataFrame):
    """
    Plot bottleneck ratios by region (Dataset 2).
    
    Answers: Deelvraag 3 - "Waar lopen projecten vast?"
    
    Args:
        df: Pijplijn dataset
    """
    logger.info("Creating bottleneck comparison chart")
    
    # Filter: Nederland excluded, calculate average per region
    regional = df[df['Regio_Naam'] != 'Nederland'].copy()
    
    # Calculate average bottleneck percentage per region
    regional_bottleneck = regional.groupby('Regio_Naam').agg({
        'Bottleneck_2Jaar_Pct': 'mean',
        'Bottleneck_5Jaar_Pct': 'mean'
    }).sort_values('Bottleneck_2Jaar_Pct', ascending=False)
    
    # Top 10 worst bottleneck regions
    top10 = regional_bottleneck.head(10)
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))
    
    x = np.arange(len(top10))
    width = 0.35
    
    # Bars for >2yr and >5yr
    bars1 = ax.barh(x - width/2, top10['Bottleneck_2Jaar_Pct'], width,
                    label='>2 jaar vast', color='#e74c3c')
    bars2 = ax.barh(x + width/2, top10['Bottleneck_5Jaar_Pct'], width,
                    label='>5 jaar vast (crisis!)', color='#c0392b')
    
    ax.set_yticks(x)
    ax.set_yticklabels(top10.index)
    ax.set_xlabel('Percentage Projecten Vastgelopen (%)', fontsize=13, fontweight='bold')
    ax.set_title('Top 10 Regio\'s met Grootste Bottlenecks (2015-2025)',
                 fontsize=16, fontweight='bold', pad=20)
    ax.legend(fontsize=11)
    ax.grid(axis='x', alpha=0.3)
    
    # Add value labels
    for bars in [bars1, bars2]:
        for bar in bars:
            width_val = bar.get_width()
            ax.text(width_val + 0.5, bar.get_y() + bar.get_height()/2,
                   f'{width_val:.1f}%', va='center', fontsize=9)
    
    plt.tight_layout()
    
    output_file = VIZ_DIR / 'bottleneck_by_region.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    logger.info(f"✓ Saved: {output_file.name}")
    
    plt.show()
    
    print("\n" + "="*60)
    print("BOTTLENECK ANALYSIS (Top 10 Worst)")
    print("="*60)
    for i, (region, row) in enumerate(top10.iterrows(), 1):
        print(f"{i:2d}. {region:30s} >2jr: {row['Bottleneck_2Jaar_Pct']:5.1f}%  >5jr: {row['Bottleneck_5Jaar_Pct']:5.1f}%")
    print("="*60 + "\n")


# Doesn't currently work
def plot_permit_vs_construction_bottleneck(df: pd.DataFrame):
    """
    Compare permit vs construction bottlenecks.
    
    Answers: Deelvraag 3 - "Waar ontstaan vertragingen? (permit vs bouw)"
    
    Args:
        df: Pijplijn dataset
    """
    logger.info("Creating permit vs construction bottleneck chart")
    
    # National level analysis
    nl_data = df[df['Regio_Naam'] == 'Nederland'].copy()
    nl_data = nl_data.sort_values(['Jaar', 'Maand'])
    nl_data['Time'] = nl_data['Jaar'] + (nl_data['Maand']-1)/12
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    # Chart 1: Bottleneck percentages over time
    ax1.plot(nl_data['Time'], nl_data['Vergunning_Bottleneck_Pct'],
            marker='o', linewidth=2, markersize=3, label='Vergunningsfase', color='#e74c3c')
    ax1.plot(nl_data['Time'], nl_data['Bouw_Bottleneck_Pct'],
            marker='s', linewidth=2, markersize=3, label='Bouwfase', color='#3498db')
    
    ax1.set_title('Bottleneck Vergelijking: Vergunning vs Bouw (2015-2025)',
                  fontsize=14, fontweight='bold')
    ax1.set_xlabel('Jaar', fontsize=12)
    ax1.set_ylabel('% Projecten >2 jaar vast', fontsize=12)
    ax1.legend(fontsize=11)
    ax1.grid(alpha=0.3)
    
    # Chart 2: Phase distribution (where are projects?)
    ax2.plot(nl_data['Time'], nl_data['Vergunning_Fase_Pct'],
            marker='o', linewidth=2, markersize=3, label='In Vergunningsfase', color='#e67e22')
    ax2.plot(nl_data['Time'], nl_data['Bouw_Fase_Pct'],
            marker='s', linewidth=2, markersize=3, label='In Bouwfase', color='#27ae60')
    
    ax2.set_title('Fase Distributie: Waar Zitten Projecten? (2015-2025)',
                  fontsize=14, fontweight='bold')
    ax2.set_xlabel('Jaar', fontsize=12)
    ax2.set_ylabel('% van Totale Pijplijn', fontsize=12)
    ax2.legend(fontsize=11)
    ax2.grid(alpha=0.3)
    
    plt.tight_layout()
    
    output_file = VIZ_DIR / 'permit_vs_construction_bottleneck.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    logger.info(f"✓ Saved: {output_file.name}")
    
    plt.show()
    
    # Summary
    print("\n" + "="*60)
    print("PERMIT VS CONSTRUCTION BOTTLENECK")
    print("="*60)
    recent = nl_data.tail(12)  # Last 12 months
    print(f"Recent average (last 12 months):")
    print(f"  Vergunningsfase bottleneck: {recent['Vergunning_Bottleneck_Pct'].mean():.1f}%")
    print(f"  Bouwfase bottleneck:        {recent['Bouw_Bottleneck_Pct'].mean():.1f}%")
    
    if recent['Vergunning_Bottleneck_Pct'].mean() > recent['Bouw_Bottleneck_Pct'].mean():
        print("\n→ VERGUNNINGSFASE = PRIMARY BOTTLENECK!")
        print("  Policy recommendation: Streamline permit process")
    else:
        print("\n→ BOUWFASE = PRIMARY BOTTLENECK!")
        print("  Policy recommendation: Support construction capacity")
    print("="*60 + "\n")


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """
    Main exploratory analysis workflow.
    
    Pattern: Similar to quick analysis in your other projects
    Purpose: Visual exploration before formal statistical tests
    
    Usage:
        python python/quick_analysis.py
    """
    
    print("\n" + "="*70)
    print("QUICK EXPLORATORY ANALYSIS")
    print("Dutch Housing Analytics (2015-2025)")
    print("="*70 + "\n")
    
    try:
        # Load Dataset 1
        df_doorlooptijden = pd.read_csv(PROCESSED_DATA_DIR / 'doorlooptijden_latest.csv')
        
        print("Creating Dataset 1 visualizations...")
        plot_temporal_trend(df_doorlooptijden)
        plot_regional_comparison(df_doorlooptijden)
        plot_housing_type_comparison(df_doorlooptijden)
        
        # Load Dataset 2 
        pijplijn_file = PROCESSED_DATA_DIR / 'woningen_pijplijn_latest.csv'
        
        if pijplijn_file.exists():
            print("\nCreating Dataset 2 visualizations...")
            df_pijplijn = pd.read_csv(pijplijn_file)
            
            plot_bottleneck_by_region(df_pijplijn)
            # plot_permit_vs_construction_bottleneck(df_pijplijn); Doesn't work for now.
        else:
            print("\n⚠️  Dataset 2 not found. Run extraction + transformation first.")
        
    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}\n")
        
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}", exc_info=True)
        print(f"\n❌ Analysis failed: {str(e)}")
        print("Check logs/pipeline.log for details\n")


if __name__ == "__main__":
    main()