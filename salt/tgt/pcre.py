# -*- coding: utf-8 -*-

# Import python libs
from __future__ import absolute_import

import logging
import re
import salt.utils.minions

log = logging.getLogger(__name__)


def check_minions(expr):
    '''
    Return the minions found by looking via regular expressions
    '''
    pki_minions = salt.utils.minions.pki_minions(__opts__)
    reg = re.compile(expr)
    return {'minions': [m for m in pki_minions if reg.match(m)],
            'missing': []}
