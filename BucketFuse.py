import stat
import errno
import os
import sys
import math
import requests
from threading import Lock

try:
    import _find_fuse_parts
except ImportError:
    pass
import json
import time
import datetime
import pytz
from fuse import FUSE, FuseOSError, Operations
from baidupcsapi import PCS
import logging
import tempfile
import progressbar

logger = logging.getLogger("BaiduFS")
formatter = logging.Formatter(
    '%(name)-12s %(asctime)s %(levelname)-8s %(message)s',
    '%a, %d %b %Y %H:%M:%S')

'''
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S')
'''
headers = {
    "accept": "application/json",
    # "Content-Type": "multipart/form-data",
    #"Content-Type": "multipart/form-data",
    "Authorization": "Token 5cbcb69efa9c67f7cab6c4a904adbcb689c43a8e"

}

class NoSuchRowException(Exception):
    pass


class NoUniqueValueException(Exception):
    pass


class ProgressBar():
    def __init__(self):
        self.first_call = True

    def __call__(self, *args, **kwargs):
        if self.first_call:
            self.widgets = [progressbar.Percentage(), ' ', progressbar.Bar(marker=progressbar.RotatingMarker('>')),
                            ' ', progressbar.FileTransferSpeed()]
            self.pbar = progressbar.ProgressBar(widgets=self.widgets, maxval=kwargs['size']).start()
            self.first_call = False

        if kwargs['size'] <= kwargs['progress']:
            self.pbar.finish()
        else:
            self.pbar.update(kwargs['progress'])


class File():
    def __init__(self):
        self.dict = {'bd_fsid': 0,
                     'bd_blocklist': 0,
                     'bd_md5': 0,
                     'st_mode': 0,
                     'st_ino': 0,
                     'st_dev': 0,
                     'st_nlink': 0,
                     'st_uid': 0,
                     'st_gid': 0,
                     'st_size': 0,
                     'st_atime': 0,
                     'st_mtime': 0,
                     'st_ctime': 0}

    def __getitem__(self, item):
        return self.dict[item]

    def __setitem__(self, key, value):
        self.dict[key] = value

    def __str__(self):
        return self.dict.__repr__()

    def __repr__(self):
        return self.dict.__repr__()

    def getDict(self):
        return self.dict


class BKFS(Operations):
    ''' netdisk filesystem'''

    def __init__(self, username, password, *args, **kw):
        self.disk = PCS(username, password)
        self.buffer = {}
        self.traversed_folder = {}
        self.bufferLock = Lock()
        self.upload_blocks = {}  # 文件上传时用于记录块的md5,{PATH:{TMP:'',BLOCKS:''}
        self.create_tmp = {}  # {outputstrem_path:file}
        self.upload_fails = {}  #
        self.fd = 3
        # 初始化云服务器
        print('设置服务器')

        pcs = "http://223.193.2.212/"
        print('pcs api server:')

        self.uploadLock = Lock()  # 上传文件时不刷新目录
        self.readLock = Lock()
        self.downloading_files = []

    def iso2timestamp(datestring, format='%Y-%m-%dT%H:%M:%S.%f+08:00', timespec='seconds'):
        """
        ISO8601时间转换为时间戳

        :param datestring:iso时间字符串 2019-03-25T16:00:00.000Z，2019-03-25T16:00:00.000111Z
        :param format:%Y-%m-%dT%H:%M:%S.%fZ；其中%f 表示毫秒或者微秒
        :param timespec:返回时间戳最小单位 seconds 秒，milliseconds 毫秒,microseconds 微秒
        :return:时间戳 默认单位秒
        """
        tz = pytz.timezone('Asia/Shanghai')
        utc_time = datetime.datetime.strptime(datestring, format)  # 将字符串读取为 时间 class datetime.datetime

        time = utc_time.replace(tzinfo=pytz.utc).astimezone(tz)

        times = {
            'seconds': int(time.timestamp()),
            'milliseconds': round(time.timestamp() * 1000),
            'microseconds': round(time.timestamp() * 1000 * 1000),
        }
        return times[timespec]

    def unlink(self, path):
        url = "http://223.193.2.212/api/v1/obj/test/"+path
        print('*' * 10, 'UNLINK CALLED', path)


        r = requests.delete(url=url, headers=headers)

    def _add_file_to_buffer(self, path, file_info):
        foo = File()

        foo['st_ctime'] = self.iso2timestamp(file_info['upt'])
        foo['st_mtime'] = self.iso2timestamp(file_info['upt'])
        foo['st_mode'] = (stat.S_IFDIR | 0o0777) if not file_info['fod'] \
            else (stat.S_IFREG | 0o0777)
        foo['st_nlink'] = 2 if not file_info['fod'] else 1
        foo['st_size'] = file_info['si']
        self.buffer[path] = foo

    def _del_file_from_buffer(self, path):
        self.buffer.pop(path)

    def getattr(self, path, fh=None):
        url = "http://223.193.2.212/api/v1/metadata/test/"+path
        ret1 = requests.get(url=url, headers=headers).content
        # print 'getattr *',path
        # 先看缓存中是否存在该文件

        if path not in self.buffer:
            print(path, '未命中')
            # print self.buffer
            # print self.traversed_folder

            jdata = json.loads(ret1)

            try:
                if 'obj' not in jdata:
                    raise FuseOSError(errno.ENOENT)
                if jdata['code_text'] != '获取元数据成功':
                    raise FuseOSError(errno.ENOENT)
                file_info = jdata['obj']
                self._add_file_to_buffer(path, file_info)
                st = self.buffer[path].getDict()
                return st
            except:
                raise FuseOSError(errno.ENOENT)
        else:
            # print path,'命中'
            return self.buffer[path].getDict()

    def readdir(self, path, offset):
        url = "http://223.193.2.212/api/v1/dir/test/" + path

        ret1 = requests.get(url=url, headers=headers).content
        self.uploadLock.acquire()
        while True:
            try:
                foo = json.loads(ret1)
                break
            except:
                print('error')

        files = ['.', '..']
        abs_files = []  # 该文件夹下文件的绝对路径
        for file in foo['files']:
            files.append(file['name'])
            abs_files.append(file['na'])
        # 缓存文件夹下文件信息,批量查询meta info

        # Update:解决meta接口一次不能查询超过100条记录
        # 分成 ceil(file_num / 100.0) 组，利用商群
        if path not in self.traversed_folder or self.traversed_folder[path] == False:
            print('正在对', path, '缓存中')

            file_num = len(abs_files)
            group = int(math.ceil(file_num / 100.0))
            for i in range(group):
                obj = [f for n, f in enumerate(abs_files) if n % group == i]  # 一组数据
                while 1:
                    try:
                        url1 = "http://223.193.2.212/api/v1/metadata/test/" + obj
                        ret = json.loads(requests.get(url=url1, headers=headers).content)
                        break
                    except:
                        print('error')

                for file_info in ret:
                    if file_info['path'] not in self.buffer:
                        self._add_file_to_buffer(file_info['path'], file_info)
            # print self.buffer
            print('对', path, '的缓存完成')

            self.traversed_folder[path] = True
        for r in files:
            yield r
        self.uploadLock.release()

    def _update_file_manual(self, path):
        url = "http://223.193.2.212/api/v1/metadata/test/"+path
        while 1:
            try:
                jdata = requests.get(url=url, headers=headers).content
                break
            except:
                print('error')

        if 'obj' not in jdata:
            raise FuseOSError(errno.ENOENT)
        file_info = jdata['obj']
        self._add_file_to_buffer(path, file_info)
