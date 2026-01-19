import argparse
import numpy as np
import pandas as pd

from pathlib import Path


def generate_synthetic_trips(n_samples: int = 10000, seed: int = 42) -> pd.DataFrame:
    """
    Generate synthetic trip data for dropoff ETA prediction.
    
    Dropoff ETA represents the time from pickup to destination,
    influenced by distance, traffic (time of day), and demand conditions.
    
    Args:
        n_samples: Number of trip records to generate
        seed: Random seed for reproducibility
    
    Returns:
        DataFrame with trip records and features
    """
    np.random.seed(seed)
    
    start_date = pd.Timestamp("2026-01-01")
    timestamps = pd.date_range(
        start=start_date, 
        periods=n_samples, 
        freq="2min"
    )
    
    # Trip distance (lognormal distribution for realistic trip lengths)
    trip_distance_km = np.random.lognormal(mean=1.5, sigma=0.7, size=n_samples)
    trip_distance_km = np.clip(trip_distance_km, 0.5, 50)
    
    # Time features
    hour_of_day = timestamps.hour
    is_weekend = (timestamps.weekday >= 5).astype(int)
    
    # Surge pressure (higher during peak hours, lower on weekends)
    base_surge = 0.3 + 0.4 * np.sin(2 * np.pi * hour_of_day / 24 - np.pi/2)
    weekend_reduction = is_weekend * 0.15
    surge_pressure = base_surge - weekend_reduction + np.random.normal(0, 0.1, n_samples)
    surge_pressure = np.clip(surge_pressure, 0, 1)
    
    # Generate dropoff ETA (target variable)
    # Base speed: ~25 km/h average in city -> 144 seconds per km
    base_speed_factor = 144  # seconds per km
    
    # Distance is the primary factor
    base_eta = trip_distance_km * base_speed_factor
    
    # Traffic effect by hour (rush hours are slower)
    # Morning rush: 7-9, Evening rush: 17-19
    morning_rush = np.exp(-0.5 * ((hour_of_day - 8) / 1.5) ** 2)
    evening_rush = np.exp(-0.5 * ((hour_of_day - 18) / 1.5) ** 2)
    rush_factor = 1 + 0.4 * (morning_rush + evening_rush)
    
    # Night hours are faster (less traffic)
    night_factor = np.where((hour_of_day >= 22) | (hour_of_day <= 5), 0.8, 1.0)
    
    # Weekend effect (generally less traffic)
    weekend_factor = np.where(is_weekend, 0.85, 1.0)
    
    # Surge effect (high demand often correlates with congestion)
    surge_slowdown = 1 + 0.2 * surge_pressure
    
    # Combine all factors
    dropoff_eta_seconds = (
        base_eta * rush_factor * night_factor * weekend_factor * surge_slowdown
    )
    
    # Add realistic noise (proportional to trip length)
    noise = np.random.normal(0, 0.1 * dropoff_eta_seconds)
    dropoff_eta_seconds += noise
    
    # Clip to reasonable bounds (1 min to 2 hours)
    dropoff_eta_seconds = np.clip(dropoff_eta_seconds, 60, 7200)
    
    # Create DataFrame
    df = pd.DataFrame({
        "timestamp": timestamps,
        "trip_distance_km": np.round(trip_distance_km, 2),
        "surge_pressure": np.round(surge_pressure, 3),
        "hour_of_day": hour_of_day,
        "is_weekend": is_weekend,
        "dropoff_eta_seconds": np.round(dropoff_eta_seconds, 0).astype(int),
    })
    
    return df


def main(n_samples: int = 10000, output_path: str = "data/dropoff_trips_madrid.parquet"):
    print(f"Generating {n_samples} synthetic dropoff trip records...")
    
    df = generate_synthetic_trips(n_samples)
    
    # Create output directory if needed
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Save to parquet
    df.to_parquet(output_file, index=False)
    print(f"Saved to {output_file}")
    
    # Print summary statistics
    print("\nDataset Summary:")
    print(f"  Records: {len(df)}")
    print(f"  Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"\nFeature statistics:")
    print(df.describe().round(2))
    
    # Print ETA distribution
    print(f"\nDropoff ETA distribution:")
    print(f"  Mean: {df['dropoff_eta_seconds'].mean():.0f} seconds ({df['dropoff_eta_seconds'].mean()/60:.1f} min)")
    print(f"  Median: {df['dropoff_eta_seconds'].median():.0f} seconds ({df['dropoff_eta_seconds'].median()/60:.1f} min)")
    print(f"  Min: {df['dropoff_eta_seconds'].min():.0f} seconds")
    print(f"  Max: {df['dropoff_eta_seconds'].max():.0f} seconds")
    
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic dropoff trip data")
    parser.add_argument(
        "--samples", 
        type=int, 
        default=10000, 
        help="Number of samples to generate"
    )
    parser.add_argument(
        "--output", 
        default="data/dropoff_trips_madrid.parquet", 
        help="Output parquet file path"
    )
    args = parser.parse_args()
    
    main(n_samples=args.samples, output_path=args.output)