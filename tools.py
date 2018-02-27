from __future__ import print_function, unicode_literals, absolute_import

import sys
import os
import time
import traceback
import codecs
import json
from cStringIO import StringIO

import base32_crockford
from git import Repo
from gherkin.tools import parse_gherkin, write_gherkin
from Crypto.Random.random import StrongRandom


class Task(object):

    def prepare(self):
        pass

    def do_run(self):
        pass

    def on_success(self):
        pass

    def on_failure(self, e):
        print_error(e)
        raise e

    def clean(self):
        pass

    def run(self):
        try:
            self.prepare()
            self.do_run()
        except Exception as e:
            self.on_failure(e)
        else:
            self.on_success()
        finally:
            self.clean()


class BuildIndexTask(Task):
    # defer init members declare here
    _repo = None
    _remote = None
    _fid_idx = None
    _sid_idx = None

    def __init__(self, path, url=None, branches=()):
        self._path = path
        self._url = url
        self._branches = branches
        self._processed_branches = set()
        self._resolved_fuids = {}
        self._resolved_suids = {}

    def prepare(self):
        if os.path.isdir(self._path):
            self._repo = Repo(self._path)
        else:
            self._repo = Repo.clone_from(self._url, self._path)
        self._repo.git.fetch()
        self._remote = self._repo.remote()
        self._fid_idx, self._sid_idx = MetaUtils.build_meta_index_from_git(self._repo)

    def process_branches(self):
        self.process_branch('master')
        for branch in self._branches:
            if branch not in self._processed_branches:
                self.process_branch(branch)

    def process_branch(self, branch_name):
        try:
            self.do_process_branch(branch_name)
        except Exception as e:
            print_error(e)
        else:
            self._processed_branches.add(branch_name)

    def do_process_branch(self, branch_name):
        self._repo.git.checkout([branch_name])
        if 'master' != branch_name:
            self._repo.git.rebase(['master'])
        paths = self.get_feature_files()
        for path in paths:
            path = os.path.join(self._repo.working_dir, path)
            self.process_file(path)

    def do_run(self):
        self.process_branches()

    def get_feature_files(self):
        stdout = self._repo.git.ls_files(['--full-name', '--', '*.feature'])
        if stdout:
            return stdout.split('\n')
        return []

    def process_file(self, path):
        try:
            self.do_process_file(path)
        except Exception as e:
            print_error(e)

    def new_fid(self):
        if len(self._fid_idx) > 0:
            return max(self._fid_idx) + 1
        return 1

    def new_sid(self, fuid):
        sub_sid_idx = [_sid for _fuid, _sid in self._sid_idx if _fuid == fuid]
        if len(sub_sid_idx) > 0:
            return max(sub_sid_idx) + 1
        return 1

    def do_process_file(self, path):
        gherkin_ast = GherkinUtils.parse_gherkin(path)
        feature = gherkin_ast['feature']
        fuid, fid = GherkinUtils.get_feature_meta(feature)
        if fuid and fid:
            fuid_set = self._fid_idx[fid]
            if len(fuid_set) > 1 and min(fuid_set) != fuid:  # handle duplication
                fid = self._resolved_fuids.get(fuid, self.new_fid())
                self._fid_idx.setdefault(fid, set()).add(fuid)
                self._resolved_fuids[fuid] = fid  # set this so that we won't resolve same fuid again
        else:  # create new meta
            fuid = new_uuid_80b()
            fid = self.new_fid()
            self._fid_idx.setdefault(fid, set()).add(fuid)
        GherkinUtils.set_feature_meta(feature, fuid, fid)

        # handle scenarios
        for child in feature['children']:
            if 'Background' == child['type']:
                continue
            suid, sid = GherkinUtils.get_scenario_meta(child)
            if suid and sid:
                suid_set = self._sid_idx[(fuid, sid)]
                if len(suid_set) > 1 and min(suid_set) != (fuid, suid):
                    sid = self._resolved_suids.get((fuid, suid),self.new_sid(fuid))
                    self._sid_idx.setdefault((fuid, sid), set()).add((fuid, suid))
                    self._resolved_suids[(fuid, suid)] = sid
            else:  # create new meta
                suid = new_uuid_80b()
                sid = self.new_sid(fuid)
                self._sid_idx.setdefault((fuid, sid), set()).add((fuid, suid))
            GherkinUtils.set_scenario_meta(child, fid, suid, sid)

        GherkinUtils.write_gherkin_with_meta(gherkin_ast, path)

        if self._repo.git.diff(['--', path]):
            self._repo.git.add([path])
            rel_path = os.path.relpath(path, self._repo.working_dir)
            self._repo.git.commit(['-m', 'meta: update file: {}'.format(rel_path)])


class GherkinUtils(object):
    @classmethod
    def new_fid_tag(cls, fid):
        return cls.new_tag("@FID.{}".format(fid))

    @classmethod
    def new_sid_tag(cls, fid, sid):
        return cls.new_tag("@SID.{}.{}".format(fid, sid))

    @classmethod
    def new_fuid_tag(cls, fuid):
        return cls.new_tag("@FUID.{}".format(fuid))

    @classmethod
    def new_suid_tag(cls, suid):
        return cls.new_tag("@SUID.{}".format(suid))

    @staticmethod
    def is_fid_tag(tag):
        return tag['name'].startswith('@FID.')

    @staticmethod
    def is_sid_tag(tag):
        return tag['name'].startswith('@SID.')

    @staticmethod
    def is_fuid_tag(tag):
        return tag['name'].startswith('@FUID.')

    @staticmethod
    def is_suid_tag(tag):
        return tag['name'].startswith('@SUID.')

    @staticmethod
    def new_tag(tag_name):
        return {
            'name': tag_name,
        }

    @classmethod
    def set_feature_meta(cls, feature_ast, fuid, fid):
        fuid_tag = cls.new_fuid_tag(fuid)
        fid_tag = cls.new_fid_tag(fid)
        tags = [fid_tag, fuid_tag] + [tag for tag in feature_ast['tags']
                                      if not (cls.is_fid_tag(tag) or cls.is_fuid_tag(tag))]
        feature_ast['tags'] = tags

    @classmethod
    def set_scenario_meta(cls, scenario_ast, fid, suid, sid):
        suid_tag = cls.new_suid_tag(suid)
        sid_tag = cls.new_sid_tag(fid, sid)
        tags = [sid_tag, suid_tag] + [tag for tag in scenario_ast['tags']
                                      if not (cls.is_sid_tag(tag) or cls.is_suid_tag(tag))]

        scenario_ast['tags'] = tags

    @classmethod
    def get_feature_meta(cls, feature_ast):
        tags = feature_ast['tags']
        fuid, fid = None, None
        for tag in tags:
            if cls.is_fuid_tag(tag):
                if fuid:
                    raise ValueError('duplicated FUID tag is found: {}'.format(tags))
                _, fuid = tag['name'].split('.', 1)
            elif cls.is_fid_tag(tag):
                if fid:
                    raise ValueError('duplicated FID tag is found: {}'.format(tags))
                _, fid = tag['name'].split('.', 1)
                fid = int(fid)
        return fuid, fid

    @classmethod
    def get_scenario_meta(cls, scenario_ast):
        tags = scenario_ast['tags']
        suid, sid = None, None
        for tag in tags:
            if cls.is_suid_tag(tag):
                if suid:
                    raise ValueError('duplicated SUID tag is found: {}'.format(tags))
                _, suid = tag['name'].split('.', 1)
            elif cls.is_sid_tag(tag):
                if sid:
                    raise ValueError('duplicated SID tag is found: {}'.format(tags))
                _, _fid, sid = tag['name'].split('.', 2)
                sid = int(sid)
        return suid, sid

    @classmethod
    def new_feature_summary(cls, feature_ast, fuid, fid):
        summary = {
            'name': feature_ast['name'],
            'description': feature_ast.get('description'),
            'tags': [tag['name'] for tag in feature_ast['tags']],
            'fuid': fuid,
            'fid': fid,
        }
        return json.dumps(summary, separators=(',', ':'))

    @classmethod
    def new_scenario_summary(cls, scenario_ast, suid, sid):
        summary = {
            'name': scenario_ast['name'],
            'description': scenario_ast.get('description'),
            'tags': [tag['name'] for tag in scenario_ast['tags']],
            'suid': suid,
            'sid': sid,
        }
        return json.dumps(summary, separators=(',', ':'))

    @classmethod
    def get_meta_lines(cls, gherkin_ast):
        lines = []
        feature = gherkin_ast['feature']
        fuid, fid = cls.get_feature_meta(feature)
        data = cls.new_feature_summary(feature, fuid, fid)
        lines.append(MetaUtils.new_feature_meta(fuid, fid, data))
        for child in feature['children']:
            if 'Background' == child['type']:
                continue
            suid, sid = cls.get_scenario_meta(child)
            data = cls.new_scenario_summary(child, suid, sid)
            lines.append(MetaUtils.new_scenario_meta(fuid, suid, sid, data))
        return lines

    @staticmethod
    def parse_gherkin(path):
        return parse_gherkin(path)

    @classmethod
    def write_gherkin_with_meta(cls, gherkin_ast, path):
        cautions = '''# CAUTIONS!
# COMMENTS START WITH "# META " AND TAGS START WITH @FID, @FUID, @SID, @SUID
# ARE CREATED AND USED BY HEARTBEATS SYSTEM
# PLEASE DO NOT ADD OR MODIFY THOSE COMMENTS AND TAGS BY HAND
'''
        with codecs.open(path, 'w', encoding='utf8') as fp:
            meta_lines = cls.get_meta_lines(gherkin_ast)
            fp.writelines(cautions)
            fp.writelines('\n'.join(meta_lines))
            fp.writelines('\n\n')
            write_gherkin(gherkin_ast, fp)


class MetaUtils(object):
    META_PATTERN = '^# META '
    META_F_PREFIX = '# META F '
    META_S_PREFIX = '# META S '

    @classmethod
    def build_meta_index_from_git(cls, repo):
        # type: (Repo) -> ...
        refs = [ref.name for ref in repo.refs]
        cmd = [cls.META_PATTERN] + refs + ['--', '*.feature']
        stdout = repo.git.grep(cmd)

        fid_idx, sid_idx = {}, {}
        if not stdout:
            return fid_idx, sid_idx

        io = StringIO(stdout)
        for line in io:
            _ref, _file_name, meta = line.split(':', 2)  # type: str
            if meta.startswith(cls.META_F_PREFIX):
                fuid, fid = cls.split_feature_meta(meta)
                fid_idx.setdefault(fid, set()).add(fuid)
            elif meta.startswith(cls.META_S_PREFIX):
                fuid, suid, sid = cls.split_scenario_meta(meta)
                sid_idx.setdefault((fuid, sid), set()).add((fuid, suid))
            else:
                raise ValueError('invalid meta line: ' + meta)
        return fid_idx, sid_idx

    @classmethod
    def get_meta_from_file(cls, path):
        meta = {}
        with codecs.open(path, 'r', encoding='utf8') as fp:
            for line in fp:
                if line.startswith(cls.META_F_PREFIX):
                    fuid, fid = cls.split_feature_meta(meta)
                    feature = {
                        'fuid': fuid,
                        'fid': fid,
                    }
                    meta['feature'] = feature
                elif line.startswith(cls.META_S_PREFIX):
                    fuid, suid, sid = cls.split_scenario_meta(meta)
                    scenario = {
                        'fuid': fuid,
                        'suid': suid,
                        'sid': sid,
                    }
                    meta.setdefault('scenarios', []).append(scenario)
        return meta

    @staticmethod
    def new_feature_meta_pattern(fuid=None):
        pattern = '^# META F'
        if fuid:
            pattern = pattern + ' ' + fuid
        return pattern

    @staticmethod
    def new_scenario_meta_pattern(fuid=None, suid=None, fuid_len=16):
        pattern = '^# META S'
        fuid_holder = '.{{{}}}'.format(fuid_len)  # e.g '.{16}'
        cond = (fuid is None, suid is None)
        if (False, True) == cond:
            return pattern + ' ' + fuid
        if (False, False) == cond:
            return pattern + ' ' + fuid + ' ' + suid
        if (True, False) == cond:
            return pattern + ' ' + fuid_holder + ' ' + suid
        return pattern

    @staticmethod
    def new_feature_meta(fuid, fid, data=''):
        return "# META F {} {:16d} {}".format(fuid, fid, data)

    @staticmethod
    def new_scenario_meta(fuid, suid, sid, data=''):
        return "# META S {} {} {:16d} {}".format(fuid, suid, sid, data)

    @staticmethod
    def split_feature_meta(meta_line):
        offset = 9  # len('# META F ') == 9
        fuid = meta_line[offset:offset+16]
        offset = 26  # len('# META F FFFFFFFFFFFFFFFF ') == 26
        fid = int(meta_line[offset:offset+16])
        return fuid, fid

    @staticmethod
    def split_scenario_meta(meta_line):
        offset = 9  # len('# META S ') == 9
        fuid = meta_line[offset:offset+16]
        offset = 26  # len('# META S FFFFFFFFFFFFFFFF ') == 26
        suid = meta_line[offset:offset+16]
        offset = 43  # len('# META F FFFFFFFFFFFFFFFF SSSSSSSSSSSSSSSS ') == 43
        sid = int(meta_line[offset:offset+16])
        return fuid, suid, sid


def new_uuid_80b():
    """
    :return: 80 bit uuid (40b time + 40b uuid) and base32 encode, len=16
    """
    time_40b = int(time.time() * 1000) & ((1 << 40) - 1)
    rand_40b = _rand.getrandbits(40)
    uuid = (1 << (40-1)) | time_40b
    uuid = (uuid << 40) | rand_40b
    return base32_crockford.encode(uuid)


def new_uuid_120b():
    """
    :return: 120 bit uuid (60b time + 60b uuid) and base32 encode, len=24
    """
    time_60b = int(time.time() * 1000) & ((1 << 60) - 1)
    rand_60b = _rand.getrandbits(60)
    uuid = (1 << (60-1)) | time_60b
    uuid = (uuid << 60) | rand_60b
    return base32_crockford.encode(uuid)


def print_error(e):
    print(e, file=sys.stderr)
    print(traceback.format_exc(), file=sys.stderr)


_rand = StrongRandom()
