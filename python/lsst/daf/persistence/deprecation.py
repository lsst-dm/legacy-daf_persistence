#
# LSST Data Management System
# Copyright 2016 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

__all__ = ("deprecateGen2", "always_warn")

import traceback
import warnings

always_warn = None
"""Control how often a warning is issued.

Options are:

* `True`: Always issue a warning.
* `False`: Only issue a warning the first time a component is encountered.
* `None`: Only issue a warning once regardless of component.
"""

_issued = {}


def deprecateGen2(component=None):
    """Issue deprecation warning for Butler.

    Parameters
    ----------
    component : `str`, optional
        Name of component to warn about. If `None` will only issue a warning
        if no other warnings have been issued.

    Notes
    -----
    The package variable `lsst.daf.persistence.deprecation.always_warn` can be
    set to `True` to always issue a warning rather than only issuing
    on first encounter. If set to `None` only a single message will ever
    be issued.
    """
    global _issued

    if always_warn:
        # Sidestep all the logic and always issue the warning
        pass
    elif always_warn is None and _issued:
        # We have already issued so return immediately
        return
    else:
        # Either we've already issued a warning for this component
        # or it's a null component and we've issued something already.
        # In this situation we do not want to warn again.
        if _issued.get(component, False) or (component is None and _issued):
            return

    # Calculate a stacklevel that pops the warning out of daf_persistence
    # and into user code.
    stacklevel = 3  # Default to the caller's caller
    stack = traceback.extract_stack()
    for i, s in enumerate(reversed(stack)):
        if not ("python/lsst/daf/persistence" in s.filename or "python/lsst/obs/base" in s.filename):
            stacklevel = i + 1
            break

    label = ""
    if component is not None and always_warn is not None:
        label = f" ({component})"

    warnings.warn(f"Gen2 Butler has been deprecated{label}. It will be removed sometime after v23.0 but no"
                  " earlier than the end of 2021.", category=FutureWarning, stacklevel=stacklevel)
    _issued[component] = True
