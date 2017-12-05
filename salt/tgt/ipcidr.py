# -*- coding: utf-8 -*-

# Import python libs
from __future__ import absolute_import

import logging

import salt.cache
import salt.defaults
import salt.utils.minions
import salt.utils.network
from salt.ext import six

if six.PY3:
    import ipaddress
else:
    import salt.ext.ipaddress as ipaddress

log = logging.getLogger(__name__)


def check_minions(expr, greedy):
    '''
    Return the minions found by looking via ipcidr
    '''
    tgt = expr
    try:
        # Target is an address?
        tgt = ipaddress.ip_address(tgt)
    except:  # pylint: disable=bare-except
        try:
            # Target is a network?
            tgt = ipaddress.ip_network(tgt)
        except:  # pylint: disable=bare-except
            log.error('Invalid IP/CIDR target: {0}'.format(tgt))
            return {'minions': [],
                    'missing': []}
    proto = 'ipv{0}'.format(tgt.version)

    def _filter_func(mdata):
        grains = mdata.get('grains')
        if grains is None or proto not in grains:
            match = False
        elif isinstance(tgt, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
            match = str(tgt) in grains[proto]
        else:
            match = salt.utils.network.in_subnet(tgt, grains[proto])

        return match

    return salt.utils.minions.check_cache_minions(greedy, lambda mdata: _filter_func(mdata), __opts__)
