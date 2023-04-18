# from abc import ABC

class Evaluator():
    def __init__(self):
        self.results = {}
        self.init()
        
    def init(self):
        self.classname = type(self).__name__
        
    def run(self):
        # global CONFIG
        FORK_DIR = '.';
        # servClass = self.classname.split('_')
        servClass = self.classname
        rulePrefix = servClass + '::rules'
        rules = []
        # rules = CONFIG.get(rulePrefix, [])
        
        ecnt = cnt = 0
        emsg = []
        methods = [method for method in dir(self) if method.startswith('__') is False and method.startswith('_check') is True]
        for method in methods:
            if not rules or str.lower(method[6:]) in rules:
                try:
                    getattr(self, method)()
                    cnt += 1
                except Exception as e:
                    ecnt += 1
                    # emsg.append(__formatException(e))
            
        if emsg:
            #__warn("Catch: {} exception(s)".format(ecnt))
            with open(FORK_DIR + '/error.txt', 'a+') as f:
                f.write('\n\n'.join(emsg))
                f.close()
                
        # scanned = CONFIG.get('scanned')
        # CONFIG.set('scanned', {
        #    'resources': scanned['resources'] + 1,
        #    'rules': scanned['rules'] + cnt,
        #    'exceptions': scanned['exceptions'] + ecnt
        #})
        
    def showInfo(self):
        print("Class: {}".format(self.classname))
        print(self.getInfo())
        # __pr(self.getInfo())
        
    def getInfo(self):
        return self.results