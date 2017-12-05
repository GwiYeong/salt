# -*- coding: utf-8 -*-

# Import python libs
from __future__ import absolute_import

import logging

import salt.defaults
import salt.utils.minions
from salt.exceptions import CommandExecutionError

HAS_RANGE = False
try:
    import seco.range  # pylint: disable=import-error
    HAS_RANGE = True
except ImportError:
    pass

log = logging.getLogger(__name__)


def check_minions(expr, greedy):
    if not HAS_RANGE:
        raise CommandExecutionError(
            'Range matcher unavailable (unable to import seco.range, '
            'module most likely not installed)'
        )
    range = seco.range.Range(__opts__['range_server'])
    try:
        return range.expand(expr)
    except seco.range.RangeException as exc:
        log.error(
            'Range exception in compound match: {0}'.format(exc)
        )
        cache_enabled = __opts__.get('minion_data_cache', False)
        if greedy:
            mlist = salt.utils.minions.get_pki_dir_minions(__opts__)
            return {'minions': mlist,
                    'missing': []}
        elif cache_enabled:
            cache = salt.cache.factory(__opts__)
            return {'minions': cache.list('minions'),
                    'missing': []}
        else:
            return {'minions': [],
                    'missing': []}
