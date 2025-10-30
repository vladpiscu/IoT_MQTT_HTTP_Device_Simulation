from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# Configure plotting style
try:
    plt.style.use('seaborn-v0_8-darkgrid')
except OSError:
    try:
        plt.style.use('seaborn-darkgrid')
    except OSError:
        plt.style.use('dark_background')
sns.set_palette("husl")


def _parse_timestamp(series: pd.Series) -> pd.Series:
    # Pandas can parse ISO 8601 with timezone directly
    return pd.to_datetime(series, errors='coerce', utc=True)


def _pick_default_device_id(df: pd.DataFrame) -> Optional[str]:
    if 'device_id' not in df.columns or df.empty:
        return None
    return df['device_id'].value_counts().idxmax()


def _filter_df(df: pd.DataFrame, *, device_id: Optional[str], sensor: Optional[str]) -> pd.DataFrame:
    out = df.copy()
    if device_id:
        out = out[out['device_id'] == device_id]
    if sensor:
        out = out[out['sensor'] == sensor]
    return out


def plot_two_devices(
    *,
    file_a: Path,
    file_b: Path,
    device_a_id: Optional[str] = None,
    device_b_id: Optional[str] = None,
    sensor: Optional[str] = None,
    output_dir: Path = Path("plots"),
) -> Tuple[Optional[Path], Optional[str]]:

    output_dir.mkdir(exist_ok=True)

    # Load CSVs
    df_a = pd.read_csv(file_a)
    df_b = pd.read_csv(file_b)

    # Choose defaults if device ids not provided
    if device_a_id is None:
        device_a_id = _pick_default_device_id(df_a)
    if device_b_id is None:
        device_b_id = _pick_default_device_id(df_b)

    warn = None
    if device_a_id is None or device_b_id is None:
        warn = "Could not infer device IDs from one or both files."

    # Filter by device and sensor
    df_a_f = _filter_df(df_a, device_id=device_a_id, sensor=sensor)
    df_b_f = _filter_df(df_b, device_id=device_b_id, sensor=sensor)

    # Parse timestamps and sort
    df_a_f = df_a_f.copy()
    df_b_f = df_b_f.copy()
    df_a_f['ts'] = _parse_timestamp(df_a_f['timestamp'])
    df_b_f['ts'] = _parse_timestamp(df_b_f['timestamp'])
    df_a_f = df_a_f.dropna(subset=['ts']).sort_values('ts')
    df_b_f = df_b_f.dropna(subset=['ts']).sort_values('ts')

    if df_a_f.empty or df_b_f.empty:
        warn = (warn + " " if warn else "") + "No data points after filtering for at least one file."

    # Labels
    label_a = f"{device_a_id or 'device A'} ({df_a_f['protocol'].iloc[0] if not df_a_f.empty else 'unknown'})"
    label_b = f"{device_b_id or 'device B'} ({df_b_f['protocol'].iloc[0] if not df_b_f.empty else 'unknown'})"

    # Plot as two separate subplots with independent time axes
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=False)

    if not df_a_f.empty:
        ax1.plot(df_a_f['ts'], df_a_f['value'], label=label_a, marker='o', linewidth=1.5, markersize=3)
        ax1.set_ylabel('Sensor Value')
        ax1.set_title(label_a)
        ax1.grid(True, alpha=0.3)

    if not df_b_f.empty:
        ax2.plot(df_b_f['ts'], df_b_f['value'], label=label_b, marker='s', linewidth=1.5, markersize=3)
        ax2.set_ylabel('Sensor Value')
        ax2.set_title(label_b)
        ax2.grid(True, alpha=0.3)

    title_sensor = f" ({sensor})" if sensor else ""
    fig.suptitle(f"Device Readings Over Time{title_sensor}", fontsize=14, fontweight='bold')
    ax2.set_xlabel('Time (UTC)')
    fig.tight_layout(rect=[0, 0, 1, 0.96])

    # Output path
    base_a = file_a.stem
    base_b = file_b.stem
    sensor_suffix = f"_{sensor}" if sensor else ""
    out_path = output_dir / f"two_devices_split_{base_a}__{base_b}{sensor_suffix}.png"
    fig.savefig(out_path, dpi=300)
    plt.close(fig)

    return out_path, warn


def main():
    import argparse

    p = argparse.ArgumentParser(description='Plot readings from two devices (two files).')
    p.add_argument('--file-a', type=str, required=True, help='Path to first CSV file (e.g., HTTP)')
    p.add_argument('--file-b', type=str, required=True, help='Path to second CSV file (e.g., MQTT)')
    p.add_argument('--device-a-id', type=str, default=None, help='Device ID in file A (optional)')
    p.add_argument('--device-b-id', type=str, default=None, help='Device ID in file B (optional)')
    p.add_argument('--sensor', type=str, default=None, help='Sensor type to filter (e.g., temperature)')
    p.add_argument('--output-dir', type=str, default='plots', help='Directory to save the plot')

    args = p.parse_args()

    file_a = Path(args.file_a)
    file_b = Path(args.file_b)
    if not file_a.exists() or not file_b.exists():
        print("One or both files do not exist.")
        return

    out_path, warn = plot_two_devices(
        file_a=file_a,
        file_b=file_b,
        device_a_id=args.device_a_id,
        device_b_id=args.device_b_id,
        sensor=args.sensor,
        output_dir=Path(args.output_dir),
    )

    if warn:
        print("Warning:", warn)
    if out_path:
        print("Saved:", out_path)


if __name__ == '__main__':
    main()


