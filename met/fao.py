
from numpy import exp, sin, pi, tan, arccos, cos, sqrt, power, minimum, maximum

#: Solar constant [ MJ m-2 min-1]
SOLAR_CONSTANT = 0.0820

# Stefan Boltzmann constant [MJ K-4 m-2 day-1]
STEFAN_BOLTZMANN_CONSTANT = 0.000000004903
"""Stefan Boltzmann constant [MJ K-4 m-2 day-1]"""

SPECIFIC_HEAT_AIR_KJ = 1.013
SPECIFIC_HEAT_AIR_MJ = 0.001013
""" Specific heat of air at constant temperature [KJ kg-1 degC-1]"""

GAS_CONSTANT = 287.
""" 287 J kg-1 K--1"""

TEST_CANOPY_RESISTANCE_SECOND = 110.
TEST_CANOPY_RESISTANCE_DAY = 0.001273
""" Senay (2013; p. 583) [s m-1]"""


# ============== AGREGATED EQUATIONS ===========================


def get_net_radiation(tmin, tmax, doy, elevation, lat, albedo):
    net_lw = net_lw_radiation(tmin=tmin, tmax=tmax, doy=doy,
                              elevation=elevation, lat=lat)
    net_sw = net_sw_radiation(elevation=elevation, albedo=albedo,
                              doy=doy, lat=lat)

    net_radiat = net_sw - net_lw
    return net_radiat


def net_lw_radiation(tmin, tmax, doy, elevation, lat):
    avp = avp_from_tmin(tmin)
    inv_esun_dist = inv_rel_dist_earth_sun(doy)
    sol_decl = sol_dec(doy)
    sunset_hr_ang = sunset_hour_angle(lat, sol_decl)
    ext_rad = et_rad(lat, sol_decl, sunset_hr_ang, inv_esun_dist)
    clear_sky_rad = cs_rad(elevation, ext_rad)
    solar_rad = sol_rad_from_t(ext_rad, clear_sky_rad, tmin, tmax,
                               coastal=False)
    lw_rad = net_out_lw_rad(tmin=tmin, tmax=tmax, sol_rad=solar_rad,
                            cs_rad=clear_sky_rad, avp=avp)
    return lw_rad


def net_sw_radiation(elevation, albedo, doy, lat):
    inv_esun_dist = inv_rel_dist_earth_sun(doy)
    sol_decl = sol_dec(doy)
    sunset_hr_ang = sunset_hour_angle(latitude=lat, sol_dec=sol_decl)
    ext_rad = et_rad(latitude=lat, sol_dec=sol_decl, sha=sunset_hr_ang,
                     ird=inv_esun_dist)
    rs = (0.75 + (2e-05 * elevation)) * ext_rad
    rns = (1 - albedo) * rs
    return rns


# =============== CONSTITUENT EQUATIONS =======================


def canopy_resistance():
    return TEST_CANOPY_RESISTANCE_DAY


def air_specific_heat():
    return SPECIFIC_HEAT_AIR_MJ


def gas_constant():
    return GAS_CONSTANT


def avp_from_tmin(tmin):
    """
    Estimate actual vapour pressure (*ea*) from minimum temperature.

    This method is to be used where humidity data are lacking or are of
    questionable quality. The method assumes that the dewpoint temperature
    is approximately equal to the minimum temperature (*tmin*), i.e. the
    air is saturated with water vapour at *tmin*.

    **Note**: This assumption may not hold in arid/semi-arid areas.
    In these areas it may be better to subtract 2 deg C from the
    minimum temperature (see Annex 6 in FAO paper).

    Based on equation 48 in Allen et al (1998).

    :param tmin: Daily minimum temperature [deg C]
    :return: Actual vapour pressure [kPa]
    :rtype: float
    """
    t_min_c = tmin - 273.15
    avp = 0.611 * exp((17.27 * t_min_c) / (t_min_c + 237.3))
    return avp


def sol_dec(day_of_year):
    """
    Calculate solar declination from day of the year.

    Based on FAO equation 24 in Allen et al (1998).

    :param day_of_year: Day of year integer between 1 and 365 or 366).
    :return: solar declination [radians]
    :rtype: float
    """
    return 0.409 * sin(((2.0 * pi / 365.0) * day_of_year - 1.39))


def sunset_hour_angle(latitude, sol_dec):
    """
    Calculate sunset hour angle (*Ws*) from latitude and solar
    declination.

    Based on FAO equation 25 in Allen et al (1998).

    :param latitude: Latitude [radians]. Note: *latitude* should be negative
        if it in the southern hemisphere, positive if in the northern
        hemisphere.
    :param sol_dec: Solar declination [radians]. Can be calculated using
        ``sol_dec()``.
    :return: Sunset hour angle [radians].
    :rtype: float
    """

    cos_sha = -tan(latitude) * tan(sol_dec)
    # If tmp is >= 1 there is no sunset, i.e. 24 hours of daylight
    # If tmp is <= 1 there is no sunrise, i.e. 24 hours of darkness
    # See http://www.itacanet.org/the-sun-as-a-source-of-energy/
    # part-3-calculating-solar-angles/
    # Domain of arccos is -1 <= x <= 1 radians (this is not mentioned in FAO-56!)
    return arccos(minimum(maximum(cos_sha, -1.0), 1.0))


def et_rad(latitude, sol_dec, sha, ird):
    """
    Estimate daily extraterrestrial radiation (*Ra*, 'top of the atmosphere
    radiation').

    Based on equation 21 in Allen et al (1998). If monthly mean radiation is
    required make sure *sol_dec*. *sha* and *irl* have been calculated using
    the day of the year that corresponds to the middle of the month.

    **Note**: From Allen et al (1998): "For the winter months in latitudes
    greater than 55 degrees (N or S), the equations have limited validity.
    Reference should be made to the Smithsonian Tables to assess possible
    deviations."

    :param latitude: Latitude [radians]
    :param sol_dec: Solar declination [radians]. Can be calculated using
        ``sol_dec()``.
    :param sha: Sunset hour angle [radians]. Can be calculated using
        ``sunset_hour_angle()``.
    :param ird: Inverse relative distance earth-sun [dimensionless]. Can be
        calculated using ``inv_rel_dist_earth_sun()``.
    :return: Daily extraterrestrial radiation [MJ m-2 day-1]
    :rtype: float
    """

    tmp1 = (24.0 * 60.0) / pi
    tmp2 = sha * sin(latitude) * sin(sol_dec)
    tmp3 = cos(latitude) * cos(sol_dec) * sin(sha)
    ext_rad = tmp1 * SOLAR_CONSTANT * ird * (tmp2 + tmp3)
    return ext_rad


def cs_rad(altitude, et_rad):
    """
    Estimate clear sky radiation from altitude and extraterrestrial radiation.

    Based on equation 37 in Allen et al (1998) which is recommended when
    calibrated Angstrom values are not available.

    :param altitude: Elevation above sea level [m]
    :param et_rad: Extraterrestrial radiation [MJ m-2 day-1]. Can be
        estimated using ``et_rad()``.
    :return: Clear sky radiation [MJ m-2 day-1]
    :rtype: float
    """
    clear_sky_rad = (0.00002 * altitude + 0.75) * et_rad
    return clear_sky_rad


def inv_rel_dist_earth_sun(day_of_year):
    """
    Calculate the inverse relative distance between earth and sun from
    day of the year.

    Based on FAO equation 23 in Allen et al (1998).

    :param day_of_year: Day of the year [1 to 366]
    :return: Inverse relative distance between earth and the sun
    :rtype: float
    """
    return 1 + (0.033 * cos((2.0 * pi / 365.0) * day_of_year))


def sol_rad_from_t(et_rad, cs_rad, tmin, tmax, coastal):
    """
    Estimate incoming solar (or shortwave) radiation, *Rs*, (radiation hitting
    a horizontal plane after scattering by the atmosphere) from min and max
    temperature together with an empirical adjustment coefficient for
    'interior' and 'coastal' regions.

    The formula is based on equation 50 in Allen et al (1998) which is the
    Hargreaves radiation formula (Hargreaves and Samani, 1982, 1985). This
    method should be used only when solar radiation or sunshine hours data are
    not available. It is only recommended for locations where it is not
    possible to use radiation data from a regional station (either because
    climate conditions are heterogeneous or data are lacking).

    **NOTE**: this method is not suitable for island locations due to the
    moderating effects of the surrounding water.

    :param et_rad: Extraterrestrial radiation [MJ m-2 day-1]. Can be
        estimated using ``et_rad()``.
    :param cs_rad: Clear sky radiation [MJ m-2 day-1]. Can be estimated
        using ``cs_rad()``.
    :param tmin: Daily minimum temperature [deg C].
    :param tmax: Daily maximum temperature [deg C].
    :param coastal: ``True`` if site is a coastal location, situated on or
        adjacent to coast of a large land mass and where air masses are
        influenced by a nearby water body, ``False`` if interior location
        where land mass dominates and air masses are not strongly influenced
        by a large water body.
    :return: Incoming solar (or shortwave) radiation (Rs) [MJ m-2 day-1].
    :rtype: float
    """
    # Determine value of adjustment coefficient [deg C-0.5] for
    # coastal/interior locations
    if coastal:
        adj = 0.19
    else:
        adj = 0.16

    sol_rad = adj * sqrt(tmax - tmin) * et_rad

    # The solar radiation value is constrained by the clear sky radiation
    return minimum(sol_rad, cs_rad)


def air_density(tmax, tmin, elevation):
    mean_temp = daily_mean_t(tmin, tmax)
    p = atm_pressure(elevation)
    virt_temp = 1.01 * (mean_temp + 273)
    rho = 3.486 * (p / virt_temp)
    return rho


def net_out_lw_rad(tmin, tmax, sol_rad, cs_rad, avp):
    """
    Estimate net outgoing longwave radiation.

    This is the net longwave energy (net energy flux) leaving the
    earth's surface. It is proportional to the absolute temperature of
    the surface raised to the fourth power according to the Stefan-Boltzmann
    law. However, water vapour, clouds, carbon dioxide and dust are absorbers
    and emitters of longwave radiation. This function corrects the Stefan-
    Boltzmann law for humidity (using actual vapor pressure) and cloudiness
    (using solar radiation and clear sky radiation). The concentrations of all
    other absorbers are assumed to be constant.

    The output can be converted to equivalent evaporation [mm day-1] using
    ``energy2evap()``.

    Based on FAO equation 39 in Allen et al (1998).

    :param tmin: Absolute daily minimum temperature [degrees Kelvin]
    :param tmax: Absolute daily maximum temperature [degrees Kelvin]
    :param sol_rad: Solar radiation [MJ m-2 day-1]. If necessary this can be
        estimated using ``sol+rad()``.
    :param cs_rad: Clear sky radiation [MJ m-2 day-1]. Can be estimated using
        ``cs_rad()``.
    :param avp: Actual vapour pressure [kPa]. Can be estimated using functions
        with names beginning with 'avp_from'.
    :return: Net outgoing longwave radiation [MJ m-2 day-1]
    :rtype: float
    """
    tmp1 = (STEFAN_BOLTZMANN_CONSTANT *
            ((power(tmax, 4) + power(tmin, 4)) / 2))
    tmp2 = (0.34 - (0.14 * sqrt(avp)))
    tmp3 = 1.35 * (sol_rad / cs_rad) - 0.35
    lw_rad = tmp1 * tmp2 * tmp3
    return lw_rad


def atm_pressure(altitude):
    """
    Estimate atmospheric pressure from altitude.

    Calculated using a simplification of the ideal gas law, assuming 20 degrees
    Celsius for a standard atmosphere. Based on equation 7, page 62 in Allen
    et al (1998).

    :param altitude: Elevation/altitude above sea level [m]
    :return: atmospheric pressure [kPa]
    :rtype: float
    """
    tmp = (293.0 - (0.0065 * altitude)) / 293.0
    return power(tmp, 5.26) * 101.3


def daily_mean_t(tmin, tmax):
    """
    Estimate mean daily temperature from the daily minimum and maximum
    temperatures.

    :param tmin: Minimum daily temperature [deg C]
    :param tmax: Maximum daily temperature [deg C]
    :return: Mean daily temperature [deg C]
    :rtype: float
    """
    return (tmax + tmin) / 2.0

# =====================================================================

