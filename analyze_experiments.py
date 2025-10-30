import os
import csv
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Set style for better-looking plots
try:
    plt.style.use('seaborn-v0_8-darkgrid')
except OSError:
    try:
        plt.style.use('seaborn-darkgrid')
    except OSError:
        plt.style.use('dark_background')
sns.set_palette("husl")


def parse_filename(filename: str) -> Optional[Tuple[int, int, str]]:

    # Remove .csv extension
    base_name = filename.replace('.csv', '')
    
    # Match pattern: number_number_PROTOCOL
    pattern = r'^(\d+)_(\d+)_(HTTP|MQTT)$'
    match = re.match(pattern, base_name, re.IGNORECASE)
    
    if match:
        message_frequency = int(match.group(1))
        num_devices = int(match.group(2))
        protocol = match.group(3).upper()
        return message_frequency, num_devices, protocol
    
    return None


def calculate_delay(row: Dict) -> Optional[float]:

    try:
        # Parse timestamps - handle both with and without timezone info
        timestamp_str = row['timestamp']
        receive_time_str = row['receive_time']
        
        # Remove timezone info if present for consistent parsing
        timestamp_str = timestamp_str.split('+')[0].split('Z')[0]
        receive_time_str = receive_time_str.split('+')[0].split('Z')[0]
        
        # Parse timestamps
        try:
            timestamp = datetime.fromisoformat(timestamp_str)
            receive_time = datetime.fromisoformat(receive_time_str)
        except ValueError:
            # Try alternative format
            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%f')
            receive_time = datetime.strptime(receive_time_str, '%Y-%m-%dT%H:%M:%S.%f')
        
        # Calculate delay in milliseconds
        delay = (receive_time - timestamp).total_seconds() * 1000
        return delay
    except Exception as e:
        print(f"Error calculating delay for row: {e}")
        return None


def load_experiment_data(data_folder: str) -> Tuple[Dict, Dict]:

    data_folder = Path(data_folder)
    
    if not data_folder.exists():
        raise FileNotFoundError(f"Data folder not found: {data_folder}")
    
    experiment_data = defaultdict(list)
    # throughput_runs holds per-file (count, span_seconds) to compute messages/sec
    throughput_runs = defaultdict(list)
    
    # Get all CSV files in the folder
    csv_files = list(data_folder.glob("*.csv"))
    
    if not csv_files:
        raise ValueError(f"No CSV files found in {data_folder}")
    
    print(f"Found {len(csv_files)} CSV files")
    
    for csv_file in csv_files:
        filename = csv_file.name
        params = parse_filename(filename)
        
        if params is None:
            print(f"Warning: Could not parse filename '{filename}', skipping...")
            continue
        
        message_frequency, num_devices, protocol = params
        print(f"Processing {filename}: freq={message_frequency}s, devices={num_devices}, protocol={protocol}")
        
        # Read CSV file
        try:
            df = pd.read_csv(csv_file)
            
            # Calculate delays for all rows
            delays = []
            for _, row in df.iterrows():
                delay = calculate_delay(row.to_dict())
                if delay is not None and delay >= 0:  # Filter out negative delays (shouldn't happen)
                    delays.append(delay)
            
            if delays:
                key = (message_frequency, num_devices, protocol)
                experiment_data[key].extend(delays)
                print(f"  → Loaded {len(delays)} messages with average delay: {np.mean(delays):.2f} ms")
            else:
                print(f"  → Warning: No valid delays found in {filename}")
            
            # Compute throughput info from receive_time
            if 'receive_time' in df.columns:
                rt = pd.to_datetime(df['receive_time'], errors='coerce', utc=True).dropna()
                if not rt.empty:
                    span_seconds = max(1e-9, (rt.max() - rt.min()).total_seconds())
                    msg_count = len(rt)
                    throughput_runs[(message_frequency, num_devices, protocol)].append((msg_count, span_seconds))
                else:
                    print(f"  → Warning: receive_time parse produced no valid timestamps in {filename}")
            else:
                print(f"  → Warning: 'receive_time' column missing in {filename}; skipping throughput computation")
                
        except Exception as e:
            print(f"Error processing {csv_file}: {e}")
            continue
    
    return experiment_data, throughput_runs


def aggregate_statistics(experiment_data: Dict) -> pd.DataFrame:
    """
    Aggregate statistics for each experiment configuration.
    
    Returns:
        DataFrame with columns: message_frequency, num_devices, protocol, 
        mean_delay, std_delay, min_delay, max_delay, median_delay, count
    """
    statistics = []
    
    for (message_frequency, num_devices, protocol), delays in experiment_data.items():
        if delays:
            statistics.append({
                'message_frequency': message_frequency,
                'num_devices': num_devices,
                'protocol': protocol,
                'mean_delay': np.mean(delays),
                'std_delay': np.std(delays),
                'min_delay': np.min(delays),
                'max_delay': np.max(delays),
                'median_delay': np.median(delays),
                'p95_delay': np.percentile(delays, 95),
                'p99_delay': np.percentile(delays, 99),
                'count': len(delays)
            })
    
    return pd.DataFrame(statistics)


def plot_delay_vs_devices_for_interval(stats_df: pd.DataFrame, *, interval_seconds: int, output_dir: str = "plots"):

    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)

    filtered = stats_df[stats_df['message_frequency'] == interval_seconds]
    if filtered.empty:
        print(f"No data found for interval {interval_seconds}s. Skipping plot.")
        return

    plt.figure(figsize=(12, 6))

    for protocol, marker in [("HTTP", "o"), ("MQTT", "s")]:
        protocol_df = filtered[filtered['protocol'] == protocol]
        if protocol_df.empty:
            continue
        grouped = protocol_df.groupby('num_devices')['mean_delay'].mean().sort_index()
        plt.plot(grouped.index, grouped.values, marker=marker, linewidth=2, label=protocol)

    plt.xlabel('Number of Devices', fontsize=12)
    plt.ylabel('Average Message Delay (ms)', fontsize=12)
    plt.title(f'Average Message Delay vs Number of Devices\n(Interval = {interval_seconds}s)', fontsize=14, fontweight='bold')
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    out_path = output_dir / f'delay_vs_devices_{interval_seconds}s.png'
    plt.savefig(out_path, dpi=300)
    plt.close()
    print(f"Saved: {out_path}")


def plot_throughput_vs_devices_for_interval(
    stats_df: pd.DataFrame,
    throughput_runs: Dict,
    *,
    interval_seconds: int,
    output_dir: str = "plots",
):
    """
    Plot messages processed per second vs number of devices for a given interval,
    comparing HTTP and MQTT on the same chart. Throughput per run is computed as
    count / span_seconds based on receive_time.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)

    # Collect average throughput per (num_devices, protocol)
    records = []
    for (freq, devices, protocol), runs in throughput_runs.items():
        if freq != interval_seconds or not runs:
            continue
        per_run_rates = [cnt / span for (cnt, span) in runs if span > 0]
        if per_run_rates:
            records.append({
                'message_frequency': freq,
                'num_devices': devices,
                'protocol': protocol,
                'throughput_mps': float(np.mean(per_run_rates)),
            })

    if not records:
        print(f"No throughput data found for interval {interval_seconds}s. Skipping plot.")
        return

    df = pd.DataFrame(records)

    plt.figure(figsize=(12, 6))
    for protocol, marker in [("HTTP", "o"), ("MQTT", "s")]:
        proto_df = df[df['protocol'] == protocol]
        if proto_df.empty:
            continue
        grouped = proto_df.groupby('num_devices')['throughput_mps'].mean().sort_index()
        plt.plot(grouped.index, grouped.values, marker=marker, linewidth=2, label=protocol)

    plt.xlabel('Number of Devices', fontsize=12)
    plt.ylabel('Messages per Second', fontsize=12)
    plt.title(f'Throughput vs Number of Devices\n(Interval = {interval_seconds}s)', fontsize=14, fontweight='bold')
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    out_path = output_dir / f'throughput_vs_devices_{interval_seconds}s.png'
    plt.savefig(out_path, dpi=300)
    plt.close()
    print(f"Saved: {out_path}")


def print_summary_statistics(stats_df: pd.DataFrame):
    """Print summary statistics to console."""
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)
    
    print("\nOverall Statistics by Protocol:")
    print("-" * 80)
    overall = stats_df.groupby('protocol').agg({
        'mean_delay': ['mean', 'std'],
        'median_delay': 'mean',
        'p95_delay': 'mean',
        'count': 'sum'
    }).round(2)
    print(overall)
    
    print("\nDetailed Statistics by Configuration:")
    print("-" * 80)
    print(stats_df.to_string(index=False))
    
    print("\n" + "="*80)


def main():
    """Main function to run the analysis."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Analyze IoT experiment results and compare HTTP vs MQTT protocols'
    )
    parser.add_argument(
        '--data-folder',
        type=str,
        default='.',
        help='Folder containing CSV experiment files (default: current directory)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='plots',
        help='Directory to save output plots (default: plots)'
    )
    parser.add_argument(
        '--export-csv',
        type=str,
        default=None,
        help='Export statistics to CSV file (optional)'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=None,
        help='Message interval (seconds). If omitted, a plot is generated for each interval found.'
    )
    
    args = parser.parse_args()
    
    print("="*80)
    print("IoT EXPERIMENT ANALYSIS TOOL")
    print("="*80)
    print(f"Data folder: {args.data_folder}")
    print(f"Output directory: {args.output_dir}")
    print()
    
    try:
        # Load experiment data
        experiment_data, throughput_runs = load_experiment_data(args.data_folder)
        
        if not experiment_data:
            print("No valid experiment data found!")
            return
        
        # Aggregate statistics
        print("\nAggregating statistics...")
        stats_df = aggregate_statistics(experiment_data)
        
        # Print summary
        print_summary_statistics(stats_df)
        
        # Create the requested plot(s): Average Delay vs Number of Devices for same interval
        print("\nGenerating plot(s): Average Delay vs Number of Devices (HTTP vs MQTT)")
        if args.interval is not None:
            plot_delay_vs_devices_for_interval(stats_df, interval_seconds=args.interval, output_dir=args.output_dir)
        else:
            for interval_seconds in sorted(stats_df['message_frequency'].unique()):
                plot_delay_vs_devices_for_interval(stats_df, interval_seconds=interval_seconds, output_dir=args.output_dir)

        # Create throughput plot(s): Messages/s vs Number of Devices for same interval
        print("\nGenerating plot(s): Throughput (messages/s) vs Number of Devices (HTTP vs MQTT)")
        if args.interval is not None:
            plot_throughput_vs_devices_for_interval(stats_df, throughput_runs, interval_seconds=args.interval, output_dir=args.output_dir)
        else:
            for interval_seconds in sorted(stats_df['message_frequency'].unique()):
                plot_throughput_vs_devices_for_interval(stats_df, throughput_runs, interval_seconds=interval_seconds, output_dir=args.output_dir)
        
        # Export CSV if requested
        if args.export_csv:
            stats_df.to_csv(args.export_csv, index=False)
            print(f"\nStatistics exported to: {args.export_csv}")
        
        print("\n" + "="*80)
        print("Analysis complete!")
        print("="*80)
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

