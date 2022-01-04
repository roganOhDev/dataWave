import os


def before_dir(pwd, num):
    path = pwd
    for i in range(num):
        path = os.path.dirname(path)
    return path


def before_abs_pwd_with_join(pwd, file, num: int = 1):
    path = before_dir(pwd,num)
    return os.path.join(path, file)
