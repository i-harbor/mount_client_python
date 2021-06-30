#!/usr/bin/env python
from __future__ import print_function, absolute_import, division
import errno
import os
import sys
import math
import requests
import logging
import progressbar
import json
import pytz
import datetime

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

headers = {
    "accept": "application/json",
    # "Content-Type": "multipart/form-data",
    # "Content-Type": "multipart/form-data",
    "Authorization": "Token 46624e723dc76c8541eb9a508742b4b066324d75"

}

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


class BucketFuse(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self, bucket, token):
        self.headers = {
            "accept": "application/json",
            # "Content-Type": "multipart/form-data",
            # "Content-Type": "multipart/form-data",
            "Authorization": "Token " + token

        }
        self.bucket = bucket
        self.buffer = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        self.buffer['/'] = dict(
            st_mode=(S_IFDIR | 0o755),
            st_ctime=now,
            st_mtime=now,
            st_atime=now,
            st_nlink=2)

    def iso2timestamp(self, datestring, format='%Y-%m-%dT%H:%M:%S.%f+08:00', timespec='seconds'):
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
        return times[timespec]-28800

    def _add_file_to_buffer(self, path, file_info):
        foo = {}
        foo['st_ctime'] = self.iso2timestamp(file_info["ult"])
        foo['st_mtime'] = self.iso2timestamp(file_info["ult"])
        foo['st_mode'] = (S_IFDIR | 0o0777) if not file_info['fod'] \
            else (S_IFREG | 0o0777)
        foo['st_nlink'] = 2 if not file_info['fod'] else 1
        foo['st_size'] = file_info['si']
        abspath = "/"+path
        self.buffer[abspath] = foo

    def _del_file_from_buffer(self, path):
        self.buffer.pop(path)

    def chmod(self, path, mode):
        self.buffer[path]['st_mode'] &= 0o770000
        self.buffer[path]['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        self.buffer[path]['st_uid'] = uid
        self.buffer[path]['st_gid'] = gid

    def create(self, path, mode):
        self.buffer[path] = dict(
            st_mode=(S_IFREG | mode),
            st_nlink=1,
            st_size=0,
            st_ctime=time(),
            st_mtime=time(),
            st_atime=time())

        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        if path not in self.buffer:
            raise FuseOSError(ENOENT)

        return self.buffer[path]

    def getxattr(self, path, name, position=0):
        attrs = self.buffer[path].get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        attrs = self.buffer[path].get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        print("***********"+path)
        url = "http://obs.cstcloud.cn/api/v1/dir/" + self.bucket + path + "/"
        ret = requests.post(url=url, headers=self.headers).content
        foo = json.loads(ret)
        if foo["code"] != 201:
            raise FuseOSError(errno.EEXIST)
        else:
            file_infos = foo["dir"]
            print("$$$$$$$$$$"+file_infos["na"])
            self._add_file_to_buffer(file_infos["na"], file_infos)

        self.buffer['/']['st_nlink'] += 1

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        if path not in self.data:
            url = "http://obs.cstcloud.cn/api/v1/obj/" + self.bucket + path+"/"
            ret = requests.get(url=url, headers=self.headers).content
            self.data[path] = ret
        print("**************self.data**************")
        print(self.data)
        return self.data[path][offset:offset + size]

    def readdir(self, path, fh):
        if path == "/":
            url = "http://obs.cstcloud.cn/api/v1/dir/" + self.bucket + "/"
        else:
            url = "http://obs.cstcloud.cn/api/v1/dir/" + self.bucket + path + "/"

        ret1 = requests.get(url=url, headers=self.headers).content
        while True:
            try:
                foo = json.loads(ret1)
                break
            except:
                print('error')
                break

        files = ['.', '..']
        abs_files = []  # 该文件夹下文件的绝对路径
        for file in foo['files']:
            files.append(file['name'])
            abs_files.append(file['na'])
        # 缓存文件夹下文件信息,批量查询meta info

        # Update:解决meta接口一次不能查询超过100条记录
        # 分成 ceil(file_num / 100.0) 组，利用商群
        # if path not in self.traversed_folder or self.traversed_folder[path] == False:
        #     print('正在对', path, '缓存中')
        #
        #     file_num = len(abs_files)
        #     group = int(math.ceil(file_num / 100.0))
        #     for i in range(group):
        #         obj = [f for n, f in enumerate(abs_files) if n % group == i]  # 一组数据
        #         while 1:
        #             try:
        #                 url1 = "http://obs.cstcloud.cn/api/v1/metadata/" + self.bucket + "/" + obj
        #                 ret = json.loads(requests.get(url=url1, headers=self.headers).content)
        #                 break
        #             except:
        #                 print('error')
        file_infos = foo["files"]
        for file_info in file_infos:
            if file_info['name'] not in self.buffer:
                self._add_file_to_buffer(file_info['na'], file_info)
            # print self.buffer
            print('对', file_info['na'], '的缓存完成')
        print(self.buffer)


        return  files

    def readlink(self, path):
        return self.data[path]


    def removexattr(self, path, name):
        attrs = self.buffer[path].get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        self.data[new] = self.data.pop(old)
        self.buffer[new] = self.buffer.pop(old)

    def rmdir(self, path):
        # with multiple level support, need to raise ENOTEMPTY if contains any files
        url = "http://obs.cstcloud.cn/api/v1/dir/" + self.bucket + path + "/"
        ret = requests.delete(url=url, headers=self.headers).content
        if ret == b'':
            self.buffer.pop(path)
            self.buffer['/']['st_nlink'] -= 1
        else:
            foo = json.loads(ret)
            raise FuseOSError(foo["code"])




    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        attrs = self.buffer[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        self.buffer[target] = dict(
            st_mode=(S_IFLNK | 0o777),
            st_nlink=1,
            st_size=len(source))

        self.data[target] = source

    def truncate(self, path, length, fh=None):
        # make sure extending the file fills in zero bytes
        self.data[path] = self.data[path][:length].ljust(
            length, '\x00'.encode('ascii'))
        self.buffer[path]['st_size'] = length

    def unlink(self, path):
        self.data.pop(path)
        self.buffer.pop(path)

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        self.buffer[path]['st_atime'] = atime
        self.buffer[path]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        self.data[path] = (
            # make sure the data gets inserted at the right offset
            self.data[path][:offset].ljust(offset, '\x00'.encode('ascii'))
            + data
            # and only overwrites the bytes that data is replacing
            + self.data[path][offset + len(data):])
        self.buffer[path]['st_size'] = len(self.data[path])
        print("*************self.data***********")
        print(data)
        print(self.buffer)
        url = "http://obs.cstcloud.cn/api/v1/obj/" + self.bucket + path+"/"
        ret = requests.put(url=url, files={"file": data}, headers=self.headers).content
        print(ret)
        # def _block_size(stream):
        #     stream.seek(0, 2)
        #     return stream.tell()
        #
        # _BLOCK_SIZE = 16 * 2 ** 20
        # # 第一块的任务
        # if offset == 0:
        #     # self.uploadLock.acquire()
        #     # self.readLock.acquire()
        #     # 初始化块md5列表
        #     self.upload_blocks[path] = {'tmp': None,
        #                                 'blocks': []}
        #     # 创建缓冲区临时文件
        #     tmp_file = tempfile.TemporaryFile('r+w+b')
        #     self.upload_blocks[path]['tmp'] = tmp_file
        #
        # # 向临时文件写入数据，检查是否>= _BLOCK_SIZE 是则上传该块并将临时文件清空
        # try:
        #     tmp = self.upload_blocks[path]['tmp']
        # except KeyError:
        #     return 0
        # tmp.write(data)
        #
        # if _block_size(tmp) > _BLOCK_SIZE:
        #     print('创建临时文件', tmp_file.name)
        #
        #     tmp.seek(0)
        #     try:
        #         foo = self.disk.upload_tmpfile(tmp, callback=ProgressBar()).content
        #         foofoo = json.loads(foo)
        #         block_md5 = foofoo['md5']
        #     except:
        #         print(foo)
        #
        #     # 在 upload_blocks 中插入本块的 md5
        #     self.upload_blocks[path]['blocks'].append(block_md5)
        #     # 创建缓冲区临时文件
        #     self.upload_blocks[path]['tmp'].close()
        #     tmp_file = tempfile.TemporaryFile('r+w+b')
        #     self.upload_blocks[path]['tmp'] = tmp_file
        #     print('创建临时文件', tmp_file.name)
        #
        # # 最后一块的任务
        # if len(data) < 4096:
        #     # 检查是否有重名，有重名则删除它
        #     while True:
        #         try:
        #             foo = self.disk.meta([path]).content
        #             foofoo = json.loads(foo)
        #             break
        #         except:
        #             print('error')
        #
        #     if foofoo['errno'] == 0:
        #         logging.debug('Deleted the file which has same name.')
        #         self.disk.delete([path])
        #     # 看看是否需要上传
        #     if _block_size(tmp) != 0:
        #         # 此时临时文件有数据，需要上传
        #         print(path, '发生上传,块末尾,文件大小', _block_size(tmp))
        #
        #         tmp.seek(0)
        #         while True:
        #             try:
        #                 foo = self.disk.upload_tmpfile(tmp, callback=ProgressBar()).content
        #                 foofoo = json.loads(foo)
        #                 break
        #             except:
        #                 print('exception, retry.')
        #
        #         block_md5 = foofoo['md5']
        #         # 在 upload_blocks 中插入本块的 md5
        #         self.upload_blocks[path]['blocks'].append(block_md5)
        #
        #     # 调用 upload_superfile 以合并块文件
        #     print('合并文件', path, type(path))
        #
        #     self.disk.upload_superfile(path, self.upload_blocks[path]['blocks'])
        #     # 删除upload_blocks中数据
        #     self.upload_blocks.pop(path)
        #     # 更新本地文件列表缓存
        #     self._update_file_manual(path)
        #     # self.readLock.release()
        #     # self.uploadLock.release()
        return len(data)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('mount')
    parser.add_argument('bucket')
    parser.add_argument('token')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(BucketFuse(args.bucket, args.token), args.mount, foreground=True, allow_other=True)

