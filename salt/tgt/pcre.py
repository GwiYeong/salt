# -*- coding: utf-8 -*-

# Import python libs
from __future__ import absolute_import

import logging
import re

log = logging.getLogger(__name__)


def check_minions(expr, pki_minions):
    '''
    Return the minions found by looking via regular expressions
    '''
    reg = re.compile(expr)
    return {'minions': [m for m in pki_minions if reg.match(m)],
            'missing': []}
