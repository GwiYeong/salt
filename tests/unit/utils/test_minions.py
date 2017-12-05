# -*- coding: utf-8 -*-

# Import python libs
from __future__ import absolute_import

# Import Salt Libs
import salt.utils.minions_origin as minions
import salt.tgt as tgts
import salt.tgt.glob as glob
import salt.loader
import tempfile
import shutil
import os

# Import Salt Testing Libs
from tests.support.mixins import LoaderModuleMockMixin
from tests.support.unit import TestCase
from tests.support.paths import TMP
from tests.support.mock import (
    patch,
    MagicMock,
)

NODEGROUPS = {
    'group1': 'L@host1,host2,host3',
    'group2': ['G@foo:bar', 'or', 'web1*'],
    'group3': ['N@group1', 'or', 'N@group2'],
    'group4': ['host4', 'host5', 'host6'],
}

EXPECTED = {
    'group1': ['L@host1,host2,host3'],
    'group2': ['G@foo:bar', 'or', 'web1*'],
    'group3': ['(', '(', 'L@host1,host2,host3', ')', 'or', '(', 'G@foo:bar', 'or', 'web1*', ')', ')'],
    'group4': ['L@host4,host5,host6'],
}


class MinionsTestCase(TestCase):
    '''
    TestCase for salt.utils.minions module functions
    '''
    def test_nodegroup_comp(self):
        '''
        Test a simple string nodegroup
        '''
        for nodegroup in NODEGROUPS:
            expected = EXPECTED[nodegroup]
            ret = minions.nodegroup_comp(nodegroup, NODEGROUPS)
            self.assertEqual(ret, expected)


fake_opt = {
    'pki_dir': TMP,
    'sock_dir': TMP,
    'transport': 'zeromq',
    'extension_modules': ''
}


class CkMinionsTestCase(TestCase, LoaderModuleMockMixin):
    '''
    TestCase for salt.utils.minions.CkMinions class
    '''
    def setup_loader_modules(self):
        return {glob: {
                '__utils__': {
                    'minions.pki_minions': MagicMock(return_value=['alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'lota', 'kappa'])
                }
            }
        }

    def setUp(self):
        self.pki_dir = tempfile.mkdtemp(dir=TMP)
        fake_opt['pki_dir'] = self.pki_dir
        # self.minions_dir = tempfile.mkdtemp(dir=os.path.join(TMP, 'minions'))
        self.ckminions = minions.CkMinions({})

    def tearDown(self):
        shutil.rmtree(self.pki_dir)
        del self.pki_dir

    def test_spec_check(self):
        # Test spec-only rule
        auth_list = ['@runner']
        ret = self.ckminions.spec_check(auth_list, 'test.arg', {}, 'runner')
        self.assertTrue(ret)
        ret = self.ckminions.spec_check(auth_list, 'test.arg', {}, 'wheel')
        self.assertFalse(ret)
        ret = self.ckminions.spec_check(auth_list, 'testarg', {}, 'runner')
        mock_ret = {'error': {'name': 'SaltInvocationError',
                              'message': 'A command invocation error occurred: Check syntax.'}}
        self.assertDictEqual(mock_ret, ret)

        # Test spec in plural form
        auth_list = ['@runners']
        ret = self.ckminions.spec_check(auth_list, 'test.arg', {}, 'runner')
        self.assertTrue(ret)
        ret = self.ckminions.spec_check(auth_list, 'test.arg', {}, 'wheel')
        self.assertFalse(ret)

        # Test spec with module.function restriction
        auth_list = [{'@runner': 'test.arg'}]
        ret = self.ckminions.spec_check(auth_list, 'test.arg', {}, 'runner')
        self.assertTrue(ret)
        ret = self.ckminions.spec_check(auth_list, 'test.arg', {}, 'wheel')
        self.assertFalse(ret)
        ret = self.ckminions.spec_check(auth_list, 'tes.arg', {}, 'runner')
        self.assertFalse(ret)
        ret = self.ckminions.spec_check(auth_list, 'test.ar', {}, 'runner')
        self.assertFalse(ret)

        # Test function name is a regex
        auth_list = [{'@runner': 'test.arg.*some'}]
        ret = self.ckminions.spec_check(auth_list, 'test.arg', {}, 'runner')
        self.assertFalse(ret)
        ret = self.ckminions.spec_check(auth_list, 'test.argsome', {}, 'runner')
        self.assertTrue(ret)
        ret = self.ckminions.spec_check(auth_list, 'test.arg_aaa_some', {}, 'runner')
        self.assertTrue(ret)

        # Test a list of funcs
        auth_list = [{'@runner': ['test.arg', 'jobs.active']}]
        ret = self.ckminions.spec_check(auth_list, 'test.arg', {}, 'runner')
        self.assertTrue(ret)
        ret = self.ckminions.spec_check(auth_list, 'jobs.active', {}, 'runner')
        self.assertTrue(ret)
        ret = self.ckminions.spec_check(auth_list, 'test.active', {}, 'runner')
        self.assertFalse(ret)
        ret = self.ckminions.spec_check(auth_list, 'jobs.arg', {}, 'runner')
        self.assertFalse(ret)

        # Test args-kwargs rules
        auth_list = [{
                '@runner': {
                    'test.arg': {
                        'args': ['1', '2'],
                        'kwargs': {
                            'aaa': 'bbb',
                            'ccc': 'ddd'
                            }
                        }
                    }
                }]
        ret = self.ckminions.spec_check(auth_list, 'test.arg', {}, 'runner')
        self.assertFalse(ret)
        args = {
                'arg': ['1', '2'],
                'kwarg': {'aaa': 'bbb', 'ccc': 'ddd'}
                }
        ret = self.ckminions.spec_check(auth_list, 'test.arg', args, 'runner')
        self.assertTrue(ret)
        args = {
                'arg': ['1', '2', '3'],
                'kwarg': {'aaa': 'bbb', 'ccc': 'ddd'}
                }
        ret = self.ckminions.spec_check(auth_list, 'test.arg', args, 'runner')
        self.assertTrue(ret)
        args = {
                'arg': ['1', '2'],
                'kwarg': {'aaa': 'bbb', 'ccc': 'ddd', 'zzz': 'zzz'}
                }
        ret = self.ckminions.spec_check(auth_list, 'test.arg', args, 'runner')
        self.assertTrue(ret)
        args = {
                'arg': ['1', '2'],
                'kwarg': {'aaa': 'bbb', 'ccc': 'ddc'}
                }
        ret = self.ckminions.spec_check(auth_list, 'test.arg', args, 'runner')
        self.assertFalse(ret)
        args = {
                'arg': ['1', '2'],
                'kwarg': {'aaa': 'bbb'}
                }
        ret = self.ckminions.spec_check(auth_list, 'test.arg', args, 'runner')
        self.assertFalse(ret)
        args = {
                'arg': ['1', '3'],
                'kwarg': {'aaa': 'bbb', 'ccc': 'ddd'}
                }
        ret = self.ckminions.spec_check(auth_list, 'test.arg', args, 'runner')
        self.assertFalse(ret)
        args = {
                'arg': ['1'],
                'kwarg': {'aaa': 'bbb', 'ccc': 'ddd'}
                }
        ret = self.ckminions.spec_check(auth_list, 'test.arg', args, 'runner')
        self.assertFalse(ret)
        args = {
                'kwarg': {'aaa': 'bbb', 'ccc': 'ddd'}
                }
        ret = self.ckminions.spec_check(auth_list, 'test.arg', args, 'runner')
        self.assertFalse(ret)
        args = {
                'arg': ['1', '2'],
                }
        ret = self.ckminions.spec_check(auth_list, 'test.arg', args, 'runner')
        self.assertFalse(ret)

        # Test kwargs only
        auth_list = [{
                '@runner': {
                    'test.arg': {
                        'kwargs': {
                            'aaa': 'bbb',
                            'ccc': 'ddd'
                            }
                        }
                    }
                }]
        ret = self.ckminions.spec_check(auth_list, 'test.arg', {}, 'runner')
        self.assertFalse(ret)
        args = {
                'arg': ['1', '2'],
                'kwarg': {'aaa': 'bbb', 'ccc': 'ddd'}
                }
        ret = self.ckminions.spec_check(auth_list, 'test.arg', args, 'runner')
        self.assertTrue(ret)

        # Test args only
        auth_list = [{
                '@runner': {
                    'test.arg': {
                        'args': ['1', '2']
                        }
                    }
                }]
        ret = self.ckminions.spec_check(auth_list, 'test.arg', {}, 'runner')
        self.assertFalse(ret)
        args = {
                'arg': ['1', '2'],
                'kwarg': {'aaa': 'bbb', 'ccc': 'ddd'}
                }
        ret = self.ckminions.spec_check(auth_list, 'test.arg', args, 'runner')
        self.assertTrue(ret)

        # Test list of args
        auth_list = [{'@runner': [{'test.arg': [{'args': ['1', '2'],
                                                 'kwargs': {'aaa': 'bbb',
                                                            'ccc': 'ddd'
                                                            }
                                                 },
                                                {'args': ['2', '3'],
                                                 'kwargs': {'aaa': 'aaa',
                                                            'ccc': 'ccc'
                                                            }
                                                 }]
                                   }]
                      }]
        args = {
                'arg': ['1', '2'],
                'kwarg': {'aaa': 'bbb', 'ccc': 'ddd'}
                }
        ret = self.ckminions.spec_check(auth_list, 'test.arg', args, 'runner')
        self.assertTrue(ret)
        args = {
                'arg': ['2', '3'],
                'kwarg': {'aaa': 'aaa', 'ccc': 'ccc'}
                }
        ret = self.ckminions.spec_check(auth_list, 'test.arg', args, 'runner')
        self.assertTrue(ret)

        # Test @module form
        auth_list = ['@jobs']
        ret = self.ckminions.spec_check(auth_list, 'jobs.active', {}, 'runner')
        self.assertTrue(ret)
        ret = self.ckminions.spec_check(auth_list, 'jobs.active', {}, 'wheel')
        self.assertTrue(ret)
        ret = self.ckminions.spec_check(auth_list, 'test.arg', {}, 'runner')
        self.assertFalse(ret)
        ret = self.ckminions.spec_check(auth_list, 'job.arg', {}, 'runner')
        self.assertFalse(ret)

        # Test @module: function
        auth_list = [{'@jobs': 'active'}]
        ret = self.ckminions.spec_check(auth_list, 'jobs.active', {}, 'runner')
        self.assertTrue(ret)
        ret = self.ckminions.spec_check(auth_list, 'jobs.active', {}, 'wheel')
        self.assertTrue(ret)
        ret = self.ckminions.spec_check(auth_list, 'jobs.active_jobs', {}, 'runner')
        self.assertTrue(ret)
        ret = self.ckminions.spec_check(auth_list, 'jobs.activ', {}, 'runner')
        self.assertFalse(ret)

        # Test @module: [functions]
        auth_list = [{'@jobs': ['active', 'li']}]
        ret = self.ckminions.spec_check(auth_list, 'jobs.active', {}, 'runner')
        self.assertTrue(ret)
        ret = self.ckminions.spec_check(auth_list, 'jobs.list_jobs', {}, 'runner')
        self.assertTrue(ret)
        ret = self.ckminions.spec_check(auth_list, 'jobs.last_run', {}, 'runner')
        self.assertFalse(ret)

        # Test @module: function with args
        auth_list = [{'@jobs': {'active': {'args': ['1', '2'],
                                           'kwargs': {'a': 'b', 'c': 'd'}}}}]
        args = {'arg': ['1', '2'],
                'kwarg': {'a': 'b', 'c': 'd'}}
        ret = self.ckminions.spec_check(auth_list, 'jobs.active', args, 'runner')
        self.assertTrue(ret)
        ret = self.ckminions.spec_check(auth_list, 'jobs.active', {}, 'runner')
        self.assertFalse(ret)

    @patch('salt.utils.minions.CkMinions._pki_minions', MagicMock(return_value=['alpha', 'beta', 'gamma']))
    def test_auth_check(self):
        # Test function-only rule
        auth_list = ['test.ping']
        ret = self.ckminions.auth_check(auth_list, 'test.ping', None, 'alpha')
        self.assertTrue(ret)
        ret = self.ckminions.auth_check(auth_list, 'test.arg', None, 'alpha')
        self.assertFalse(ret)

        # Test minion and function
        auth_list = [{'alpha': 'test.ping'}]
        ret = self.ckminions.auth_check(auth_list, 'test.ping', None, 'alpha')
        self.assertTrue(ret)
        ret = self.ckminions.auth_check(auth_list, 'test.arg', None, 'alpha')
        self.assertFalse(ret)
        ret = self.ckminions.auth_check(auth_list, 'test.ping', None, 'beta')
        self.assertFalse(ret)

        # Test function list
        auth_list = [{'*': ['test.*', 'saltutil.cmd']}]
        ret = self.ckminions.auth_check(auth_list, 'test.arg', None, 'alpha')
        self.assertTrue(ret)
        ret = self.ckminions.auth_check(auth_list, 'test.ping', None, 'beta')
        self.assertTrue(ret)
        ret = self.ckminions.auth_check(auth_list, 'saltutil.cmd', None, 'gamma')
        self.assertTrue(ret)
        ret = self.ckminions.auth_check(auth_list, 'saltutil.running', None, 'gamma')
        self.assertFalse(ret)

        # Test an args and kwargs rule
        auth_list = [{
                'alpha': {
                    'test.arg': {
                        'args': ['1', '2'],
                        'kwargs': {
                            'aaa': 'bbb',
                            'ccc': 'ddd'
                            }
                        }
                    }
                }]
        ret = self.ckminions.auth_check(auth_list, 'test.arg', None, 'runner')
        self.assertFalse(ret)
        ret = self.ckminions.auth_check(auth_list, 'test.arg', [], 'runner')
        self.assertFalse(ret)
        args = ['1', '2', {'aaa': 'bbb', 'ccc': 'ddd', '__kwarg__': True}]
        ret = self.ckminions.auth_check(auth_list, 'test.arg', args, 'runner')
        self.assertTrue(ret)
        args = ['1', '2', '3', {'aaa': 'bbb', 'ccc': 'ddd', 'eee': 'fff', '__kwarg__': True}]
        ret = self.ckminions.auth_check(auth_list, 'test.arg', args, 'runner')
        self.assertTrue(ret)
        args = ['1', {'aaa': 'bbb', 'ccc': 'ddd', '__kwarg__': True}]
        ret = self.ckminions.auth_check(auth_list, 'test.arg', args, 'runner')
        self.assertFalse(ret)
        args = ['1', '2', {'aaa': 'bbb', '__kwarg__': True}]
        ret = self.ckminions.auth_check(auth_list, 'test.arg', args, 'runner')
        self.assertFalse(ret)
        args = ['1', '3', {'aaa': 'bbb', 'ccc': 'ddd', '__kwarg__': True}]
        ret = self.ckminions.auth_check(auth_list, 'test.arg', args, 'runner')
        self.assertFalse(ret)
        args = ['1', '2', {'aaa': 'bbb', 'ccc': 'fff', '__kwarg__': True}]
        ret = self.ckminions.auth_check(auth_list, 'test.arg', args, 'runner')
        self.assertFalse(ret)

        # Test kwargs only rule
        auth_list = [{
                'alpha': {
                    'test.arg': {
                        'kwargs': {
                            'aaa': 'bbb',
                            'ccc': 'ddd'
                            }
                        }
                    }
                }]
        args = ['1', '2', {'aaa': 'bbb', 'ccc': 'ddd', '__kwarg__': True}]
        ret = self.ckminions.auth_check(auth_list, 'test.arg', args, 'runner')
        self.assertTrue(ret)
        args = [{'aaa': 'bbb', 'ccc': 'ddd', 'eee': 'fff', '__kwarg__': True}]
        ret = self.ckminions.auth_check(auth_list, 'test.arg', args, 'runner')
        self.assertTrue(ret)

        # Test args only rule
        auth_list = [{
                'alpha': {
                    'test.arg': {
                        'args': ['1', '2'],
                        }
                    }
                }]
        args = ['1', '2', {'aaa': 'bbb', 'ccc': 'ddd', '__kwarg__': True}]
        ret = self.ckminions.auth_check(auth_list, 'test.arg', args, 'runner')
        self.assertTrue(ret)
        args = ['1', '2']
        ret = self.ckminions.auth_check(auth_list, 'test.arg', args, 'runner')
        self.assertTrue(ret)

    def test_modularize_test_glob(self):
        ckminions = minions.CkMinions(fake_opt)
        utils = salt.loader.utils(fake_opt)
        # patch lazy loaded module
        with patch('salt.utils.minions.CkMinions._pki_minions', MagicMock(
            return_value=['alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'lota', 'kappa'])) \
            , patch.dict(utils, {'minions.pki_minions': MagicMock(
            return_value=['alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'lota', 'kappa'])}):
            tgt_modules = salt.loader.tgt(fake_opt, utils)
            modularized_ckminions = tgts.CkMinions(fake_opt)
            with patch.object(modularized_ckminions, 'tgts', tgt_modules):
                ckminion_ret = ckminions.check_minions('a*', 'glob')
                modularized_ckminion_ret = modularized_ckminions.check_minions('a*', 'glob')
                self.assertEqual(sorted(ckminion_ret['minions']), sorted(modularized_ckminion_ret['minions']))
                self.assertEqual(sorted(ckminion_ret['missing']), sorted(modularized_ckminion_ret['missing']))
                self.assertEqual(sorted(modularized_ckminion_ret['minions']), sorted(['alpha']))

    def test_modularize_test_list(self):
        ckminions = minions.CkMinions(fake_opt)
        utils = salt.loader.utils(fake_opt)
        # patch lazy loaded module
        with patch('salt.utils.minions.CkMinions._pki_minions', MagicMock(
            return_value=['alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'lota', 'kappa'])) \
            , patch.dict(utils, {'minions.pki_minions': MagicMock(
            return_value=['alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'lota', 'kappa'])}):
            tgt_modules = salt.loader.tgt(fake_opt, utils)
            modularized_ckminions = tgts.CkMinions(fake_opt)
            with patch.object(modularized_ckminions, 'tgts', tgt_modules):
                ckminion_ret = ckminions.check_minions('alpha,beta', 'list')
                modularized_ckminion_ret = modularized_ckminions.check_minions('alpha,beta', 'list')
                self.assertEqual(sorted(ckminion_ret['minions']), sorted(modularized_ckminion_ret['minions']))
                self.assertEqual(sorted(ckminion_ret['missing']), sorted(modularized_ckminion_ret['missing']))
                self.assertEqual(sorted(modularized_ckminion_ret['minions']), sorted(['alpha', 'beta']))

    def test_modularize_test_pcre(self):
        ckminions = minions.CkMinions(fake_opt)
        utils = salt.loader.utils(fake_opt)
        # patch lazy loaded module
        with patch('salt.utils.minions.CkMinions._pki_minions', MagicMock(
            return_value=['alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'lota', 'kappa'])) \
            , patch.dict(utils, {'minions.pki_minions': MagicMock(
            return_value=['alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'lota', 'kappa'])}):
            tgt_modules = salt.loader.tgt(fake_opt, utils)
            modularized_ckminions = tgts.CkMinions(fake_opt)
            with patch.object(modularized_ckminions, 'tgts', tgt_modules):
                ckminion_ret = ckminions.check_minions('.*ta', 'pcre')
                modularized_ckminion_ret = modularized_ckminions.check_minions('.*ta', 'pcre')
                self.assertEqual(sorted(ckminion_ret['minions']), sorted(modularized_ckminion_ret['minions']))
                self.assertEqual(sorted(ckminion_ret['missing']), sorted(modularized_ckminion_ret['missing']))
                self.assertEqual(sorted(modularized_ckminion_ret['minions']),
                                 sorted(['beta', 'delta', 'zeta', 'eta', 'theta', 'lota']))
