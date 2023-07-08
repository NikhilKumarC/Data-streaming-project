class SecretsManage:
    def __init__(self,access=None,secret=None):
        self.access=access
        self.secret=secret

    def getaccessKey(self):
        return self.access

    def getsecretKey(self):
        return self.secret
