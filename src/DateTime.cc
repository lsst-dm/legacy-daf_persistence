// -*- lsst-c++ -*-


/** \file
 * \brief Implementation of DateTime class.
 *
 * \author $Author: ktlim $
 * \version $Revision: 2151 $
 * \date $Date$
 *
 * Contact: Kian-Tat Lim (ktl@slac.stanford.edu)
 *
 * \ingroup mwi
 */

#ifndef __GNUC__
#  define __attribute__(x) /*NOTHING*/
#endif
static char const* SVNid __attribute__((unused)) = "$Id$";

#include "lsst/mwi/persistence/DateTime.h"


namespace lsst {
namespace mwi {
namespace persistence {

/// Epoch = 1970 JAN  1 00:00:00 = JD 2440587.5 = MJD 40587.0
static double const EPOCH_IN_MJD = 40587.0;

/// Nanoseconds per day.
static double const NSEC_PER_DAY = 86.4e12;

/// Nanoseconds per day as a long long.
static long long const LL_NSEC_PER_DAY = 86400000000000LL;

/// Leap second descriptor.
struct Leap {
    int when; ///< Number of days since the epoch.
    int secs; ///< Leap seconds added to TAI after midnight on that day.
};

/* Leap second table
 *
 * Source: ftp://maia.usno.navy.mil/ser7/tai-utc.dat
 *
 * 1972 JAN  1 =JD 2441317.5  TAI-UTC=  10.0
 * 1972 JUL  1 =JD 2441499.5  TAI-UTC=  11.0
 * 1973 JAN  1 =JD 2441683.5  TAI-UTC=  12.0
 * 1974 JAN  1 =JD 2442048.5  TAI-UTC=  13.0
 * 1975 JAN  1 =JD 2442413.5  TAI-UTC=  14.0
 * 1976 JAN  1 =JD 2442778.5  TAI-UTC=  15.0
 * 1977 JAN  1 =JD 2443144.5  TAI-UTC=  16.0
 * 1978 JAN  1 =JD 2443509.5  TAI-UTC=  17.0
 * 1979 JAN  1 =JD 2443874.5  TAI-UTC=  18.0
 * 1980 JAN  1 =JD 2444239.5  TAI-UTC=  19.0
 * 1981 JUL  1 =JD 2444786.5  TAI-UTC=  20.0
 * 1982 JUL  1 =JD 2445151.5  TAI-UTC=  21.0
 * 1983 JUL  1 =JD 2445516.5  TAI-UTC=  22.0
 * 1985 JUL  1 =JD 2446247.5  TAI-UTC=  23.0
 * 1988 JAN  1 =JD 2447161.5  TAI-UTC=  24.0
 * 1990 JAN  1 =JD 2447892.5  TAI-UTC=  25.0
 * 1991 JAN  1 =JD 2448257.5  TAI-UTC=  26.0
 * 1992 JUL  1 =JD 2448804.5  TAI-UTC=  27.0
 * 1993 JUL  1 =JD 2449169.5  TAI-UTC=  28.0
 * 1994 JUL  1 =JD 2449534.5  TAI-UTC=  29.0
 * 1996 JAN  1 =JD 2450083.5  TAI-UTC=  30.0
 * 1997 JUL  1 =JD 2450630.5  TAI-UTC=  31.0
 * 1999 JAN  1 =JD 2451179.5  TAI-UTC=  32.0
 * 2006 JAN  1 =JD 2453736.5  TAI-UTC=  33.0
 */

/** Table of leap seconds to date since the epoch, in ascending order.
 * Source: ftp://maia.usno.navy.mil/ser7/tai-utc.dat
 */
static Leap leapSecTable[] = {
      {730, 10},      {912, 11},     {1096, 12},     {1461, 13},
     {1826, 14},     {2191, 15},     {2557, 16},     {2922, 17},
     {3287, 18},     {3652, 19},     {4199, 20},     {4564, 21},
     {4929, 22},     {5660, 23},     {6574, 24},     {7305, 25},
     {7670, 26},     {8217, 27},     {8582, 28},     {8947, 29},
     {9496, 30},    {10043, 31},    {10592, 32},    {13149, 33}
};
/// Number of leap second descriptors in the table.
static int const LEAP_SECS = sizeof(leapSecTable) / sizeof(Leap);


/** Constructor.
 * \param[in] nsecs Number of nanoseconds since the epoch in UTC or TAI.
 */
DateTime::DateTime(long long nsecs) : _nsecs(nsecs) {
}

/** Constructor.
 * \param[in] mjd Modified Julian Day in UTC.
 */
DateTime::DateTime(double mjd) {
    _nsecs = static_cast<long long>((mjd - EPOCH_IN_MJD) * NSEC_PER_DAY);
}

/** Accessor.
 * \return Number of nanoseconds since the epoch in UTC or TAI.
 */
long long DateTime::nsecs(void) const {
        return _nsecs;
}

/** Convert UTC time to TAI time.
 * \return A DateTime object with leap seconds removed.
 *
 * The application must remember which time system was used to construct each
 * DateTime.
 */
DateTime DateTime::utc2tai(void) const {
    if (_nsecs < leapSecTable[0].when * LL_NSEC_PER_DAY) {
        double leapsecs = (utc2mjd() - 39126.0) * 0.002592 + 4.21317;
        return DateTime(_nsecs - static_cast<long long>(1.0e9 * leapsecs));
    }
    for (int i = 1; i < LEAP_SECS - 1; ++i) {
        if (_nsecs < leapSecTable[i].when * LL_NSEC_PER_DAY) {
            return DateTime(_nsecs - leapSecTable[i - 1].secs * 1000000000LL);
        }
    }
    return DateTime(_nsecs - leapSecTable[LEAP_SECS - 1].secs * 1000000000LL);
}

/** Convert TAI time to UTC time.
 * \return A DateTime object with leap seconds added.
 *
 * The application must remember which time system was used to construct each
 * DateTime.
 */
DateTime DateTime::tai2utc(void) const {
    if (_nsecs < leapSecTable[0].when * LL_NSEC_PER_DAY +
        leapSecTable[0].secs * 1000000000LL) {
        return DateTime(static_cast<long long>(
            (static_cast<double>(_nsecs) - 4.21317e9 -
             (EPOCH_IN_MJD - 39126.0) * 0.002592e9) /
            (1 - 0.002592e9 / NSEC_PER_DAY)));
    }
    for (int i = 1; i < LEAP_SECS - 1; ++i) {
        if (_nsecs < leapSecTable[i].when * LL_NSEC_PER_DAY +
            leapSecTable[i].secs * 1000000000LL) {
            return DateTime(_nsecs + leapSecTable[i - 1].secs * 1000000000LL);
        }
    }
    return DateTime(_nsecs + leapSecTable[LEAP_SECS - 1].secs * 1000000000LL);
}

/** Convert UTC time to Modified Julian Day.
 * \return The Modified Julian Day corresponding to the time, which is assumed
 * to be UTC.
 *
 * The application must remember which time system was used to construct each
 * DateTime.
 */
double DateTime::utc2mjd(void) const {
    return static_cast<double>(_nsecs) / NSEC_PER_DAY + EPOCH_IN_MJD;
}

/** Convert TAI time to Modified Julian Day.
 * \return The Modified Julian Day corresponding to the time, which is assumed
 * to be TAI.
 *
 * The application must remember which time system was used to construct each
 * DateTime.
 */
double DateTime::tai2mjd(void) const {
    return static_cast<double>(tai2utc().nsecs()) / NSEC_PER_DAY + EPOCH_IN_MJD;
}

/** Convert UTC time to struct tm.
 * \return Structure with decoded time in UTC.
 */
struct tm DateTime::utc2gmtime(void) const {
    struct tm gmt;
    time_t secs = static_cast<time_t>(_nsecs / 1000000000LL);
    gmtime_r(&secs, &gmt);
    return gmt;
}

/** Convert time to struct timespec.
 * \return Structure with time in seconds and nanoseconds.
 */
struct timespec DateTime::timespec(void) const {
    struct timespec ts;
    ts.tv_sec = static_cast<time_t>(_nsecs / 1000000000LL);
    ts.tv_nsec = static_cast<int>(_nsecs % 1000000000LL);
    return ts;
}

/** Convert time to struct timeval.
 * \return Structure with time in seconds and microseconds.
 */
struct timeval DateTime::timeval(void) const {
    struct timeval tv;
    tv.tv_sec = static_cast<time_t>(_nsecs / 1000000000LL);
    tv.tv_usec = static_cast<int>((_nsecs % 10000000000LL) / 1000);
    return tv;
}

}}} // namespace lsst::mwi::persistence
