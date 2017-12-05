# -*- coding: utf-8 -*-

# Import python libs
from __future__ import absolute_import

import logging

import salt.defaults
import salt.utils.minions

log = logging.getLogger(__name__)


def check_minions(expr, delimiter, greedy):
    return salt.utils.minions.check_cache_minions(greedy, lambda mdata: _filter_func(mdata, expr, delimiter), __opts__)


def check_pcre_minions(expr, delimiter, greedy):
    '''
    Return the minions found by looking via pillar with PCRE
    '''
    return salt.utils.minions.check_cache_minions(greedy,
                                                  lambda mdata: _filter_func(mdata, expr, delimiter, regex_match=True),
                                                  __opts__)


def check_exact_minions(expr, delimiter, greedy):
    '''
     Return the minions found by looking via pillar
     '''
    return salt.utils.minions.check_cache_minions(greedy,
                                                  lambda mdata: _filter_func(mdata, expr, delimiter, exact_match=True),
                                                  __opts__)


def _filter_func(mdata, expr, delimiter, regex_match=False, exact_match=False):
    search_results = mdata.get('pillar')
    return salt.utils.data.subdict_match(search_results,
                                         expr,
                                         delimiter=delimiter,
                                         regex_match=regex_match,
                                         exact_match=exact_match)
