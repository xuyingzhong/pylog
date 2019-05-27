import os,threading,time

import socket,redis,json

files_path = '/home/deploy/log/'
exclude_lines = []
# include_lines = ['Tomcat started on','ERROR','WARN']
include_lines = []
files_list = []
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(('8.8.8.8', 80))
ip = s.getsockname()[0]
r = redis.Redis(host='172.16.0.131', port=6379)


def sendtoredis(index, date_str, log_type, message):
    try:
        data = {
            "_index": index,
            "_type": "applog",
            "_source": {"timestamp": date_str, "log_type": log_type, 'message': message, 'ip': ip}
        }
        r.lpush('applog',json.dumps(data))
    except:
        pass


def redfile(files_path, file_name):
    file = os.path.join(files_path, file_name)
    file_seek = os.path.join(os.getcwd(), 'logseek', file_name.split('.log')[0] + '.txt')
    size_nu = 0
    while True:
        try:
            fsize = os.stat(file).st_size
        except:
            fsize = 0
        try:
            with open(file_seek, 'r') as seek:
                where = int(seek.read())
                if fsize < where:
                    where = 0
        except:
            where = 0
        if fsize == where:
            size_nu += 1
            if size_nu >= 1440:
                if fsize == 0:
                    del files_list[files_list.index(file_name)]
                    break
                time.sleep(60)
                continue
            elif size_nu >= 600:
                time.sleep(30)
                continue
            else:
                time.sleep(1)
                continue
        with open(file, 'r') as accf:
            size_nu = 0
            accf.seek(where)
            not_nu = 0
            index = ''
            date_str = ''
            log_type = ''
            message = ''
            while True:
                line = accf.readline().strip()
                where = accf.tell()
                if not len(line):
                    not_nu += 1
                    if not_nu >= 50:
                        break
                    else:
                        continue
                else:
                    not_nu = 0
                    if line.startswith('20'):
                        if not index == '':
                            if not len(exclude_lines) and not len(include_lines):
                                sendtoredis(index, date_str, log_type, message)
                            elif len(exclude_lines) and not len(include_lines):
                                for i in exclude_lines:
                                    if i not in line:
                                        sendtoredis(index, date_str, log_type, message)
                            elif not len(exclude_lines) and len(include_lines):
                                for i in include_lines:
                                    if i in line:
                                        sendtoredis(index, date_str, log_type, message)
                            else:
                                for i in exclude_lines:
                                    if i not in line:
                                        sendtoredis(index, date_str, log_type, message)
                                for i in include_lines:
                                    if i in line:
                                        sendtoredis(index, date_str, log_type, message)
                        line_list = line.split()
                        index = file_name + '-' + line_list[0]
                        date_str = line_list[0] + 'T' + line_list[1] + '+0800'
                        log_type = line_list[2]
                        message = ' '.join(line_list[3:])
                    else:
                        if message == '':
                            break
                        message = message + '\n' + line
                        continue
        with open(file_seek, 'w') as seekf:
            seekf.write(str(where))


if __name__ == '__main__':
    while True:
        try:
            files = os.listdir(files_path)
            for file_name in files:
                file_name_list = file_name.split('.')
                if file_name_list[-1] == 'log' and file_name_list[-2] != 'warn':
                    if file_name not in files_list:
                        t = threading.Thread(target=redfile, args=(files_path, file_name))
                        t.start()
                        files_list.append(file_name)
        except:
            pass
        time.sleep(5)



