import logging
from ucloud.core import exc
from ucloud.client import Client
import json

logger = logging.getLogger("ucloud")
logger.setLevel(logging.WARN)
import re
import requests
# 从config文件导入public_key 和private_key
from config import *
from loguru import logger

# 不需要备份的DBId
exclude_DBId_list = [
    # "udbv-hfhtjegj",#"backend_1",
    # "udbhhjarfegion",#"backend_data_v1",
    # "udbfs-glblqh0rq",#backend-dev
]


def get_every_preject(public_key, private_key):
    client = Client({
        "public_key": public_key,
        "private_key": private_key,
        "base_url": "https://api.ucloud.cn"
    })

    try:
        resp = client.uaccount().get_project_list({
        })
    except exc.UCloudException as e:
        print(e)

    projects = resp.get("ProjectSet")
    for project in projects:
        project = project.get("ProjectId")
        yield project


def get_udb_id(project_id):
    client = Client({
        "region": "cn-bj2",
        "public_key": public_key,
        "private_key": private_key,
        "project_id": project_id,
        "base_url": "https://api.ucloud.cn"
    })
    try:
        resp = client.udb().describe_udb_instance({
            "ClassType": "SQL",
            "Offset": 0,
            "Limit": 30
        })
    except exc.UCloudException as e:
        print(e)
    else:
        # print(resp)
        DBId_dict_list = resp.get("DataSet")
        for DBId_dict in DBId_dict_list:
            DBId = DBId_dict.get("DBId")
            DBName = DBId_dict.get("Name")
            yield DBId, DBName


def get_udb_BackupId(project_id, DBId):
    client = Client({
        "region": "cn-bj2",
        "public_key": public_key,
        "private_key": private_key,
        "project_id": project_id,
        "base_url": "https://api.ucloud.cn"
    })

    try:
        resp = client.udb().describe_udb_backup({
            "Offset": 0,
            "Limit": 30,
            "DBId": DBId,

        })
    except exc.UCloudException as e:
        print(e)
    else:
        # print(resp)
        BackupId = resp.get("DataSet")[0].get("BackupId")
        # logger.info(BackupId)
        return BackupId


def get_udb_backup_url(ProjectId, DBId, BackupId):
    client = Client({
        "region": "cn-bj2",
        "public_key": public_key,
        "private_key": private_key,
        "project_id": ProjectId,
        "base_url": "https://api.ucloud.cn"
    })

    try:
        resp = client.udb().describe_udb_instance_backup_url({
            "DBId": DBId,
            "BackupId": BackupId,

        })
    except exc.UCloudException as e:
        print(e)
    else:
        # print(resp)
        BackupURL = resp.get("BackupPath")
        InnerBackupPath = resp.get("InnerBackupPath")
        # logger.info(InnerBackupPath)
        # logger.info(BackupURL)
        return InnerBackupPath, BackupURL


if __name__ == '__main__':
    # 获得所有的项目ID
    p_id = get_every_preject(public_key, private_key)
    for project_id in p_id:
        logger.info(project_id)
        # 获得该项目下的所有 DBId和DBName
        DBId_Name_list = get_udb_id(project_id)
        for DBId, DBName in DBId_Name_list:
            if DBId not in exclude_DBId_list:  # 排除本次不需要备份的udb_id
                # 根据DBId获得BackupId
                BackupId = get_udb_BackupId(project_id, DBId)
                # 根据DBId、BackupId得到 backup_url(外网)
                backup_url = get_udb_backup_url(project_id, DBId, BackupId)[0]  # 内网
                # backup_url = get_udb_backup_url(project_id,DBId,BackupId)[1] #外网
                pattern = r'(http|https)://(.*?)/(.*?)/(.*?)\?'
                name_re = re.match(pattern, backup_url)
                # logger.info(name_re)
                filename = path + project_id + "." + DBName + "." + name_re.group(4)
                logger.info(filename)
                r = requests.get(backup_url, stream=True)
                with open(filename, "wb") as f:
                    count = 0
                    for chunk in r.iter_content(chunk_size=102400):  # 1024 bytes
                        if chunk:
                            count += 1
                            print(count)
                            f.write(chunk)
            else:
                logger.warning("该DB在不备份列表中，不需要备份")
