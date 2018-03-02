from __future__ import print_function, unicode_literals, absolute_import

from unittest import TestCase
from gherkin_utils.tools import MetaUtils


class TestMetaUtils(TestCase):
    def test_new_scenario_meta_pattern(self):
        fuid = 'F' * 16
        suid = 'S' * 16
        self.assertEqual(MetaUtils.new_scenario_meta_pattern(), '^# META S')
        self.assertEqual(MetaUtils.new_scenario_meta_pattern(None, fuid), '^# META S FFFFFFFFFFFFFFFF')
        self.assertEqual(MetaUtils.new_scenario_meta_pattern(suid, fuid), '^# META S FFFFFFFFFFFFFFFF SSSSSSSSSSSSSSSS')
        self.assertEqual(MetaUtils.new_scenario_meta_pattern(suid), '^# META S .{16} SSSSSSSSSSSSSSSS')

    def test_meta_line(self):
        fuid = 'F' * 16
        suid = 'S' * 16
        f_meta_line = MetaUtils.new_feature_meta(fuid, 12345, 'any data')
        s_meta_line = MetaUtils.new_scenario_meta(fuid, suid, 54321, 'any data')
        self.assertEqual(MetaUtils.split_feature_meta(f_meta_line), (fuid, 12345, 'any data'))
        self.assertEqual(MetaUtils.split_scenario_meta(s_meta_line), (fuid, suid, 54321, 'any data'))
