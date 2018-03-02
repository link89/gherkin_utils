from __future__ import print_function, unicode_literals, absolute_import

import sys
import os
import time
import traceback
import codecs
import json
from cStringIO import StringIO

import base32_crockford
import git.exc
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

    @classmethod
    def labeling_file_in_repo(cls, repo_path, file_path):
        task = cls(repo_path, fetch_remote=False)
        task.prepare()
        task.process_file(file_path, create_commit=False)

    def __init__(self, path, url=None, branches=(), fetch_remote=True, rebase_to=None, push_to_remote=False):
        self._path = path
        self._url = url
        self._branches = branches
        self._processed_branches = set()
        self._resolved_fuids = {}
        self._resolved_suids = {}
        self._fetch_remote=fetch_remote
        self._push_to_remote = push_to_remote
        self._rebase_to = rebase_to

    def prepare(self):
        if os.path.isdir(self._path):
            self._repo = Repo(self._path)
        else:
            self._repo = Repo.clone_from(self._url, self._path)
        if self._fetch_remote:
            self._repo.git.fetch()
        self._remote = self._repo.remote()
        self._fid_idx, self._sid_idx = MetaUtils.git_build_meta_index(self._repo)

    def process_branches(self):
        if self._rebase_to is not None:
            self.process_branch(self._rebase_to)
        for branch in self._branches:
            if branch not in self._processed_branches:
                self.process_branch(branch)

    def process_branch(self, branch_name):
        try:
            self.do_process_branch(branch_name)
        except Exception as e:
            print_error(e)
        finally:
            self._processed_branches.add(branch_name)

    def do_process_branch(self, branch_name):
        self._repo.git.checkout([branch_name])
        if self._rebase_to is not None and self._rebase_to != branch_name:
            self._repo.git.rebase([self._rebase_to])
        paths = self.get_feature_files()
        for path in paths:
            path = os.path.join(self._repo.working_dir, path)
            self.process_file(path)
        if self._push_to_remote:
            self._remote.push(branch_name)

    def do_run(self):
        self.process_branches()

    def get_feature_files(self):
        stdout = self._repo.git.ls_files(['--full-name', '--', '*.feature'])
        if stdout:
            return stdout.split('\n')
        return []

    def process_file(self, path, create_commit=True):
        try:
            self.do_process_file(path, create_commit)
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

    def do_process_file(self, path, create_commit):
        gherkin_ast = GherkinUtils.parse_gherkin(path)
        feature = gherkin_ast['feature']
        fuid, fid = GherkinUtils.get_feature_meta(feature)
        if fuid is not None and fid is not None:
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
            if suid is not None and sid is not None:
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

        if create_commit and self._repo.git.diff(['--', path]):
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
    def get_gherkin_meta(cls, gherkin_ast):
        meta_data = {}
        if 'feature' not in gherkin_ast:
            return meta_data
        feature = gherkin_ast['feature']
        fuid, fid = cls.get_feature_meta(feature)
        feature_meta = cls.new_feature_summary(feature, fuid, fid)
        meta_data['feature'] = feature_meta

        children = []
        for child in feature['children']:
            if 'Background' == child['type']:
                continue
            suid, sid = cls.get_scenario_meta(child)
            data = cls.new_scenario_summary(child, suid, sid)
            children.append(data)
        meta_data['children'] = children
        return meta_data

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
    def new_feature_summary(cls, feature_ast, fuid, fid, to_json=False):
        summary = {
            'name': feature_ast['name'],
            'description': feature_ast.get('description'),
            'tags': [tag['name'] for tag in feature_ast['tags']],
            'fuid': fuid,
            'fid': fid,
        }
        if to_json:
            return json.dumps(summary, separators=(',', ':'))
        return summary

    @classmethod
    def new_scenario_summary(cls, scenario_ast, suid, sid, to_json=False):
        summary = {
            'name': scenario_ast['name'],
            'description': scenario_ast.get('description'),
            'tags': [tag['name'] for tag in scenario_ast['tags']],
            'suid': suid,
            'sid': sid,
            'type': scenario_ast['type'],
        }
        if to_json:
            return json.dumps(summary, separators=(',', ':'))
        return summary

    @classmethod
    def new_meta_lines(cls, gherkin_ast):
        lines = []
        if 'feature' not in gherkin_ast:
            return lines
        feature = gherkin_ast['feature']
        fuid, fid = cls.get_feature_meta(feature)
        if fuid is not None and fid is not None:
            data = cls.new_feature_summary(feature, fuid, fid, to_json=True)
            lines.append(MetaUtils.new_feature_meta(fuid, fid, data))
        for child in feature['children']:
            if 'Background' == child['type']:
                continue
            suid, sid = cls.get_scenario_meta(child)
            if suid is not None and sid is not None:
                data = cls.new_scenario_summary(child, suid, sid, to_json=True)
                lines.append(MetaUtils.new_scenario_meta(fuid, suid, sid, data))
        return lines

    @classmethod
    def new_meta_header(cls, gherkin_ast):
        cautions = '''# CAUTIONS!
# COMMENTS START WITH "# META " AND TAGS START WITH @FID, @FUID, @SID, @SUID
# ARE CREATED AND USED BY HEARTBEATS SYSTEM
# PLEASE DO NOT ADD OR MODIFY THOSE COMMENTS AND TAGS BY HAND
'''
        meta_lines = cls.new_meta_lines(gherkin_ast)
        return cautions + '\n'.join(meta_lines)

    @staticmethod
    def parse_gherkin(path):
        return parse_gherkin(path)

    @classmethod
    def write_gherkin_with_meta(cls, gherkin_ast, path):
        with codecs.open(path, 'w', encoding='utf8') as fp:
            meta_header = cls.new_meta_header(gherkin_ast)
            fp.writelines(meta_header)
            fp.writelines('\n\n')
            write_gherkin(gherkin_ast, fp)


class MetaUtils(object):
    META_PATTERN = '^# META '
    META_F_PREFIX = '# META F '
    META_S_PREFIX = '# META S '

    @staticmethod
    def git_grep_features(repo, pattern, refs=None):
        # type: (Repo, basestring, ...) -> ...
        if isinstance(refs, list):
            cmd = [pattern] + refs + ['--', '*.feature']
        elif isinstance(refs, basestring):
            cmd = [pattern, refs, '--', '*.feature']
        else:
            cmd = [pattern, '--', '*.feature']
        stdout = ''
        try:
            stdout = repo.git.grep(cmd)
        except git.exc.GitCommandError as e:
            if e.status != 1:  # git grep will return status 1 when nothing is match
                raise e
        return stdout

    @classmethod
    def git_get_features_meta(cls, repo, refs=None, fuid=None, skip_error=False):
        pattern = cls.new_feature_meta_pattern(fuid)
        stdout = cls.git_grep_features(repo, pattern, refs)
        io = StringIO(stdout)
        features = []
        for line in io:
            try:
                _ref, _file_name, meta = line.split(':', 2)
                if meta.startswith(cls.META_F_PREFIX):
                    fuid, fid, data = cls.split_feature_meta(meta)
                    summary = json.loads(data)
                    summary['_ref'] = _ref
                    summary['_file_name'] = _file_name
                    features.append(summary)
            except Exception as e:
                if not skip_error:
                    raise e
                else:
                    print_error(e)
        return features

    @classmethod
    def git_build_meta_index(cls, repo):
        # type: (Repo) -> ...
        refs = [ref.name for ref in repo.refs]
        stdout = cls.git_grep_features(repo, cls.META_PATTERN, refs)

        fid_idx, sid_idx = {}, {}
        if not stdout:
            return fid_idx, sid_idx

        io = StringIO(stdout)
        for line in io:
            _ref, _file_name, meta = line.split(':', 2)  # type: str
            if meta.startswith(cls.META_F_PREFIX):
                fuid, fid, _data = cls.split_feature_meta(meta)
                fid_idx.setdefault(fid, set()).add(fuid)
            elif meta.startswith(cls.META_S_PREFIX):
                fuid, suid, sid, _data = cls.split_scenario_meta(meta)
                sid_idx.setdefault((fuid, sid), set()).add((fuid, suid))
            else:
                raise ValueError('invalid meta line: ' + meta)
        return fid_idx, sid_idx

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
        offset = 43  # len('# META F FFFFFFFFFFFFFFFF                1 ') == 26
        data = meta_line[offset:]
        return fuid, fid, data

    @staticmethod
    def split_scenario_meta(meta_line):
        offset = 9  # len('# META S ') == 9
        fuid = meta_line[offset:offset+16]
        offset = 26  # len('# META S FFFFFFFFFFFFFFFF ') == 26
        suid = meta_line[offset:offset+16]
        offset = 43  # len('# META F FFFFFFFFFFFFFFFF SSSSSSSSSSSSSSSS ') == 43
        sid = int(meta_line[offset:offset+16])
        offset = 60  # len('# META F FFFFFFFFFFFFFFFF SSSSSSSSSSSSSSSS                1 ') == 60
        data = meta_line[offset:]
        return fuid, suid, sid, data


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
