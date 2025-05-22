import numpy as np

INVALID_VALUE = -999.0

def db_dt_processing(
        db_tau: np.ndarray, 
        dt_tau: np.ndarray
    ):
    """
    Processes two aerosol arrays (DB and DT) and generates three different datasets:
    1. `dbdt_tau` where DB is replaced with DT when DB is invalid and DT is valid.
    2. `dtdb_tau` where DT is replaced with DB when DB is valid and DT is invalid.
    3. `avg_tau` which is the average of DB and DT when both are valid, or DT where DB is invalid. This array is initialized as DB

    Args:
        db_tau (np.ndarray): A 2D numpy array DB (Deep Blue).
        dt_tau (np.ndarray): A 2D numpy array DT (Dark Target).

    Returns:
        tuple: A tuple containing three 2D numpy arrays:
            - `dbdt_tau`
            - `dtdb_tau`
            - `avg_tau`
    """

    # Preferred DB
    dbdt_tau = db_tau.copy()
    mask_db_invalid_dt_valid = (db_tau == INVALID_VALUE) & (dt_tau != INVALID_VALUE)
    dbdt_tau[mask_db_invalid_dt_valid] = dt_tau[mask_db_invalid_dt_valid]

    # Preferred DT
    dtdb_tau = dt_tau.copy()
    mask_dt_invalid_db_valid = (dt_tau == INVALID_VALUE) & (db_tau != INVALID_VALUE)
    dtdb_tau[mask_dt_invalid_db_valid] = db_tau[mask_dt_invalid_db_valid]
    
    # Simple Average
    avg_tau = db_tau.copy()
    mask_both_valid = (dt_tau != INVALID_VALUE) & (db_tau != INVALID_VALUE)
    avg_tau[mask_both_valid] = (dt_tau[mask_both_valid] + db_tau[mask_both_valid]) / 2.0

    mask_avg_invalid_db_valid = (avg_tau == INVALID_VALUE) & (dt_tau != INVALID_VALUE)
    avg_tau[mask_avg_invalid_db_valid] = dt_tau[mask_avg_invalid_db_valid]

    return dbdt_tau, dtdb_tau, avg_tau