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

__all__ = ("deprecateGen2", "always_warn", "deprecate_class")

import textwrap
import traceback
import warnings

always_warn = False
"""Control how often a warning is issued.

Options are:

* `True`: Always issue a warning.
* `False`: Only issue a warning the first time a component is encountered.
* `None`: Only issue a warning once regardless of component.
"""

# Cache recording which components have issued previously
_issued = {}

# This is the warning message to issue. There is a "label" placeholder
# that should be inserted on format.
_warning_msg = "Gen2 Butler has been deprecated{label}. "\
               " This Gen2 code may be removed in any future daily or weekly release."

_version_deprecated = "v22.0"


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

    warnings.warn(_warning_msg.format(label=label), category=FutureWarning, stacklevel=stacklevel)
    _issued[component] = True


def _add_deprecation_docstring(wrapped):
    """Add the deprecation docstring to the supplied class"""
    # Add the deprecation message to the docstring.
    # (logic taken from deprecated.sphinx)
    reason = textwrap.dedent(_warning_msg.format(label="")).strip()
    reason = '\n'.join(
        textwrap.fill(line, width=70, initial_indent='   ',
                      subsequent_indent='   ') for line in reason.splitlines()
    ).strip()

    docstring = textwrap.dedent(wrapped.__doc__ or "")

    if docstring:
        docstring += "\n\n"
    docstring += f".. deprecated:: {_version_deprecated}\n"

    # No need for a component label since this message will be associated
    # with the class directly.
    docstring += "   {reason}\n".format(reason=reason)
    wrapped.__doc__ = docstring
    return wrapped


class Deprecator:
    """Class for deprecation decorator to use."""

    def __call__(self, wrapped):
        """Intercept the call to ``__new__`` and issue the warning first."""
        old_new1 = wrapped.__new__

        def wrapped_cls(cls, *args, **kwargs):
            deprecateGen2(cls.__name__)
            if old_new1 is object.__new__:
                return old_new1(cls)
            return old_new1(cls, *args, **kwargs)

        wrapped.__new__ = staticmethod(wrapped_cls)
        _add_deprecation_docstring(wrapped)

        # Want to add the deprecation message to subclasses as well
        # so register an __init_subclass__ method to attach it.
        # We know that daf_persistence does not use __init_subclass_
        # so do not need to get any cleverer here.
        def add_deprecation_docstring_to_subclass(cls, **kwargs):
            _add_deprecation_docstring(cls)

        wrapped.__init_subclass__ = classmethod(add_deprecation_docstring_to_subclass)

        return wrapped


def deprecate_class(cls):
    return Deprecator()(cls)
