def assemble_eta_features(
    trip_distance_km: float,
    features_5m: dict,
) -> dict:
    """
    Assemble feature vector for ETA prediction.
    
    Combines trip-specific features with marketplace features
    from the 5-minute aggregation window.
    
    Args:
        trip_distance_km: Estimated trip distance
        features_5m: Derived features from 5-minute window
    
    Returns:
        Feature dictionary ready for ETA model prediction
    """
    return {
        "trip_distance_km": trip_distance_km,
        "supply_demand_ratio": features_5m["supply_demand_ratio"],
        "surge_pressure": features_5m["surge_pressure"],
        "deadhead_km_avg": features_5m["deadhead_km_avg"],
        "available_drivers": features_5m["available_drivers"],
        "ride_requests": features_5m["ride_requests"],
    }