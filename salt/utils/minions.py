# -*- coding: utf-8 -*-
'''
This module contains routines used to verify the matcher against the minions
expected to return
'''

# Import python libs
from __future__ import absolute_import

import logging
import os
import re

import salt.auth.ldap
import salt.cache
import salt.payload
import salt.tgt
import salt.utils.data
import salt.utils.files
import salt.utils.network
import salt.utils.stringutils
import salt.utils.versions
from salt.ext import six


log = logging.getLogger(__name__)

TARGET_REX = re.compile(
        r'''(?x)
        (
            (?P<engine>G|P|I|J|L|N|S|E|R)  # Possible target engines
            (?P<delimiter>(?<=G|P|I|J).)?  # Optional delimiter for specific engines
        @)?                                # Engine+delimiter are separated by a '@'
                                           # character and are optional for the target
        (?P<pattern>.+)$'''                # The pattern passed to the target engine
    )


def parse_target(target_expression):
    '''Parse `target_expressing` splitting it into `engine`, `delimiter`,
     `pattern` - returns a dict'''

    match = TARGET_REX.match(target_expression)
    if not match:
        log.warning('Unable to parse target "{0}"'.format(target_expression))
        ret = {
            'engine': None,
            'delimiter': None,
            'pattern': target_expression,
        }
    else:
        ret = match.groupdict()
    return ret


def get_minion_data(minion, opts):
    '''
    Get the grains/pillar for a specific minion.  If minion is None, it
    will return the grains/pillar for the first minion it finds.

    Return value is a tuple of the minion ID, grains, and pillar
    '''
    grains = None
    pillar = None
    if opts.get('minion_data_cache', False):
        cache = salt.cache.factory(opts)
        if minion is None:
            for id_ in cache.list('minions'):
                data = cache.fetch('minions/{0}'.format(id_), 'data')
                if data is None:
                    continue
        else:
            data = cache.fetch('minions/{0}'.format(minion), 'data')
        if data is not None:
            grains = data.get('grains', None)
            pillar = data.get('pillar', None)
    return minion if minion else None, grains, pillar


def nodegroup_comp(nodegroup, nodegroups, skip=None, first_call=True):
    '''
    Recursively expand ``nodegroup`` from ``nodegroups``; ignore nodegroups in ``skip``

    If a top-level (non-recursive) call finds no nodegroups, return the original
    nodegroup definition (for backwards compatibility). Keep track of recursive
    calls via `first_call` argument
    '''
    expanded_nodegroup = False
    if skip is None:
        skip = set()
    elif nodegroup in skip:
        log.error('Failed nodegroup expansion: illegal nested nodegroup "{0}"'.format(nodegroup))
        return ''

    if nodegroup not in nodegroups:
        log.error('Failed nodegroup expansion: unknown nodegroup "{0}"'.format(nodegroup))
        return ''

    nglookup = nodegroups[nodegroup]
    if isinstance(nglookup, six.string_types):
        words = nglookup.split()
    elif isinstance(nglookup, (list, tuple)):
        words = nglookup
    else:
        log.error('Nodegroup \'%s\' (%s) is neither a string, list nor tuple',
                  nodegroup, nglookup)
        return ''

    skip.add(nodegroup)
    ret = []
    opers = ['and', 'or', 'not', '(', ')']
    for word in words:
        if not isinstance(word, six.string_types):
            word = str(word)
        if word in opers:
            ret.append(word)
        elif len(word) >= 3 and word.startswith('N@'):
            expanded_nodegroup = True
            ret.extend(nodegroup_comp(word[2:], nodegroups, skip=skip, first_call=False))
        else:
            ret.append(word)

    if ret:
        ret.insert(0, '(')
        ret.append(')')

    skip.remove(nodegroup)

    log.debug('nodegroup_comp({0}) => {1}'.format(nodegroup, ret))
    # Only return list form if a nodegroup was expanded. Otherwise return
    # the original string to conserve backwards compat
    if expanded_nodegroup or not first_call:
        return ret
    else:
        opers_set = set(opers)
        ret = words
        if (set(ret) - opers_set) == set(ret):
            # No compound operators found in nodegroup definition. Check for
            # group type specifiers
            group_type_re = re.compile('^[A-Z]@')
            if not [x for x in ret if '*' in x or group_type_re.match(x)]:
                # No group type specifiers and no wildcards. Treat this as a
                # list of nodenames.
                joined = 'L@' + ','.join(ret)
                log.debug(
                    'Nodegroup \'%s\' (%s) detected as list of nodenames. '
                    'Assuming compound matching syntax of \'%s\'',
                    nodegroup, ret, joined
                )
                # Return data must be a list of compound matching components
                # to be fed into compound matcher. Enclose return data in list.
                return [joined]

        log.debug(
            'No nested nodegroups detected. Using original nodegroup '
            'definition: %s', nodegroups[nodegroup]
        )
        return ret


def get_acc(opts):
    # TODO: this is actually an *auth* check
    if opts.get('transport', 'zeromq') in ('zeromq', 'tcp'):
        return 'minions'
    else:
        return 'accepted'

def pki_minions(opts):

    '''
    Retreive complete minion list from PKI dir.
    Respects cache if configured
    '''
    acc = get_acc(opts)
    serial = salt.payload.Serial(opts)
    minions = []
    pki_cache_fn = os.path.join(opts['pki_dir'], acc, '.key_cache')
    try:
        if opts['key_cache'] and os.path.exists(pki_cache_fn):
            log.debug('Returning cached minion list')
            with salt.utils.files.fopen(pki_cache_fn) as fn_:
                return serial.load(fn_)
        else:
            minions = get_pki_dir_minions(opts)
        return minions
    except OSError as exc:
        log.error('Encountered OSError while evaluating  minions in PKI dir: {0}'.format(exc))
        return minions


def get_pki_dir_minions(opts):
    acc = get_acc(opts)
    minions = []
    for fn_ in salt.utils.data.sorted_ignorecase(os.listdir(os.path.join(opts['pki_dir'], acc))):
        if not fn_.startswith('.') and os.path.isfile(os.path.join(opts['pki_dir'], acc, fn_)):
            minions.append(fn_)

    return minions


def check_cache_minions(greedy, filter_func, opts):
    '''
    Helper function to search for minions in master caches
    If 'greedy' return accepted minions that matched by the condition or absend in the cache.
    If not 'greedy' return the only minions have cache data and matched by the condition.
    '''
    cache_enabled = opts.get('minion_data_cache', False)
    cache = salt.cache.factory(opts)
    if greedy:
        minions = get_pki_dir_minions(opts)
    elif cache_enabled:
        minions = cache.list('minions')
    else:
        return {'minions': [],
                'missing': []}

    if cache_enabled:
        if greedy:
            cminions = cache.list('minions')
        else:
            cminions = minions
        if not cminions:
            return {'minions': minions,
                    'missing': []}
        minions = set(minions)
        for id_ in cminions:
            if greedy and id_ not in minions:
                continue
            mdata = cache.fetch('minions/{0}'.format(id_), 'data')
            if mdata is None:
                if not greedy:
                    minions.remove(id_)
                continue

            if not filter_func(mdata):
                minions.remove(id_)

            # search_results = mdata.get(search_type)
            # if not salt.utils.data.subdict_match(search_results,
            #                                      expr,
            #                                      delimiter=delimiter,
            #                                      regex_match=regex_match,
            #                                      exact_match=exact_match):

        minions = list(minions)
    return {'minions': minions,
            'missing': []}


def mine_get(tgt, fun, tgt_type='glob', opts=None):
    '''
    Gathers the data from the specified minions' mine, pass in the target,
    function to look up and the target type
    '''
    ret = {}
    serial = salt.payload.Serial(opts)
    checker = salt.tgt.CkMinions(opts)
    _res = checker.check_minions(
            tgt,
            tgt_type)
    minions = _res['minions']
    cache = salt.cache.factory(opts)
    for minion in minions:
        mdata = cache.fetch('minions/{0}'.format(minion), 'mine')
        if mdata is None:
            continue
        fdata = mdata.get(fun)
        if fdata:
            ret[minion] = fdata
    return ret