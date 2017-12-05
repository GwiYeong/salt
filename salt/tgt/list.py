# -*- coding: utf-8 -*-

# Import python libs
from __future__ import absolute_import

import logging
from salt.ext import six
import salt.utils.minions

log = logging.getLogger(__name__)


def check_minions(expr):  # pylint: disable=unused-argument
    log.info('list.check_minoins is called')
    '''
    Return the minions found by looking via a list
    '''
    pki_minions = salt.utils.minions.pki_minions(__opts__)
    if isinstance(expr, six.string_types):
        expr = [m for m in expr.split(',') if m]
    return {'minions': [x for x in expr if x in pki_minions],
            'missing': [x for x in expr if x not in pki_minions]}