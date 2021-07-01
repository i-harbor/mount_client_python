mount_client_python
=====
introduction
-----
基于obs.cstcloud.cn里提供的api接口实现的Linux系统下云盘挂载
    mkdir ./mnt  (创建一个空文件夹用于挂载)
    python bucketfuse.py mount bucket_name token  (运行脚本进行挂载)
需要的库
-----
fusepy: https://github.com/terencehonles/fusepy
request
progressbar
