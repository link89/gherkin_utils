from __future__ import print_function, unicode_literals, absolute_import

import sys
import os
import time
import traceback

import base32_crockford
from git import Repo
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

    def __init__(self, path, url=None, branches=()):
        self._path = path
        self._url = url
        self._branches = branches
        self._processed_branches = set()

    def prepare(self):
        if os.path.isdir(self._path):
            self._repo = Repo(self._path)
        else:
            self._repo = Repo.clone_from(self._url, self._path)
        self._repo.git.fetch()
        self._remote = self._repo.remote()
        # TODO: fetch exists meta records here


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

    def do_run(self):
        self.process_branches()


class MetaUtils(object):
    def __init__(self, repo):
        self._repo = repo  # type: Repo

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
    def new_fid_tag(fid):
        return "@F.{}".format(fid)

    @staticmethod
    def new_sid_tag(fid, sid):
        return "@S.{}.{}".format(fid, sid)

    @staticmethod
    def new_fuid_tag(fuid):
        return "@FUID.{}".format(fuid)

    @staticmethod
    def new_suid_tag(suid):
        return "@SUID.{}".format(suid)

    @staticmethod
    def new_feature_meta(fuid, fid, data=''):
        return "# META F {} {:16d} ".format(fuid, fid, data)

    @staticmethod
    def new_scenario_meta(fuid, suid, sid, data=''):
        return "# META S {} {} {:16d} {}".format(fuid, suid, sid, data)



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
