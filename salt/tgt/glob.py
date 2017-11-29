# -*- coding: utf-8 -*-

# Import python libs
from __future__ import absolute_import
import logging
import salt.defaults
import fnmatch

log = logging.getLogger(__name__)


def check_minions(expr, pki_minions):
    log.info('glob.check_minions is called')
    return {'minions': fnmatch.filter(pki_minions, expr), 'missing': []}
