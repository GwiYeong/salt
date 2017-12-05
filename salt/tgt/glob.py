# -*- coding: utf-8 -*-

# Import python libs
from __future__ import absolute_import
import logging
import salt.defaults
import fnmatch
import salt.utils.minions

log = logging.getLogger(__name__)


def check_minions(expr):
    pki_minions = salt.utils.minions.pki_minions(__opts__)
    return {'minions': fnmatch.filter(pki_minions, expr), 'missing': []}
