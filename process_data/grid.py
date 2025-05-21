import numpy as np
from tqdm import tqdm
from typing import Tuple

def grid(
    limit: list[float], 
    gsize: float, 
    indata: list[float], 
    inlat: list[float], 
    inlon: list[float],
	vza: list[float]
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
	"""
    Processes data into a specified grid and computes aggregated statistics.

    Args:
        limit (list[float]): A list defining the geographical limits of the grid 
            in the format [min_lat, max_lat, min_lon, max_lon].
        gsize (float): The grid size, defining the resolution in degrees.
        indata (list[float]): The input data values (e.g., aerosol optical thickness).
        inlat (list[float]): The corresponding latitudes for the input data.
        inlon (list[float]): The corresponding longitudes for the input data.

    Returns:
        Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]: 
            A tuple containing the following 2D arrays:
            - avgtau (np.ndarray): Average values in each grid cell.
            - stdtau (np.ndarray): Standard deviation of values in each grid cell.
            - grdlat (np.ndarray): Latitudes of the grid cells.
            - grdlon (np.ndarray): Longitudes of the grid cells.
            - mintau (np.ndarray): Minimum values in each grid cell.
            - maxtau (np.ndarray): Maximum values in each grid cell.
            - count (np.ndarray): Count of data points in each grid cell.
			- vza (np.ndarray): Averaged Viewing zenith angle in each grid cell.
    """	
	dy=gsize
	dx=gsize
	minlat=float(limit[0])
	maxlat=float(limit[1])
	minlon=float(limit[2])
	maxlon=float(limit[3])
	xdim=round(1+((maxlon-minlon)/dx))
	ydim=round(1+((maxlat-minlat)/dy))
	sumtau=np.zeros((xdim,ydim), dtype=np.float32)
	sum_vza = np.zeros((xdim,ydim), dtype=np.float32)
	sqrtau=np.zeros((xdim,ydim), dtype=np.float32)
	count=np.zeros((xdim,ydim), dtype=int)
	mintau=np.full([xdim,ydim], 10, dtype=np.float32)
	maxtau=np.full([xdim,ydim], -1, dtype=np.float32)
	avgtau=np.full([xdim,ydim], -999.0, dtype=np.float32)
	avg_vza = np.full([xdim,ydim], -999.0, dtype=np.float32)
	stdtau=np.full([xdim,ydim], -999.0, dtype=np.float32)
	grdlat=np.full([xdim,ydim], -999.0, dtype=np.float32)
	grdlon=np.full([xdim,ydim], -999.0, dtype=np.float32)
	
	for ii in tqdm(range(len(indata)), desc="Gridding data"):
		if (inlat[ii]>=minlat and inlat[ii] <= maxlat and inlon[ii]>= minlon and inlon[ii] <= maxlon):
			i=int((inlon[ii]-minlon)/dx)
			j=int((inlat[ii]-minlat)/dy)
			sumtau[i,j]=sumtau[i,j]+indata[ii]
			sum_vza[i,j] = sum_vza[i,j] + vza[ii]

			sqrtau[i,j]=sqrtau[i,j]+(indata[ii])**2
			count[i,j]+=1
			if indata[ii] < mintau[i,j]:
				mintau[i,j]=indata[ii]
			if indata[ii] > maxtau[i,j]:
				maxtau[i,j]=indata[ii]
				
	for i in tqdm(range(xdim), desc="Calculating averages"):
		for j in range(ydim):
			grdlon[i,j]=dx*i+minlon
			grdlat[i,j]=dx*j+minlat
			if count[i,j] > 0:
				avgtau[i,j]=sumtau[i,j]/count[i,j]
				avg_vza[i,j] = sum_vza[i,j] / count[i,j]
				para1 = (1 / count[i, j]) * (sqrtau[i, j] + count[i, j] * avgtau[i, j] *avgtau[i, j] - 2 * avgtau[i, j] * sumtau[i, j])
				if para1 > 0:
					stdtau[i,j]=np.sqrt(para1)

	mintau[mintau == 10] = -1
	count[count == 0] = -999
	count = count.astype(np.int32)
	
	return avgtau, stdtau, grdlat, grdlon, mintau, maxtau, count, avg_vza