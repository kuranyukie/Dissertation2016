# -*- coding: utf-8 -*-  
import sys, os; _paths = filter(lambda _ : _.split('/')[-1] in ['src', 'HelloWord'], [os.path.realpath(__file__ + '/..' * (_ + 1)) for _ in range(os.path.realpath(__file__).count('/'))]); sys.path.extend([_[0] for _ in os.walk(_paths[0])] + [_[0] for _ in os.walk(_paths[1] + '/HelloWord-Lib/src')])
from util import *

class CircularCurler :

    def __init__(self) :
        self.clear()

    def clear(self) :
        self._flog        = sys.stdout
        self._num_working = 0
        self._timeout     = None
        self._is_quiet    = True
        self._tasks       = Queue()
        self._result_list = []
        self._trash_list  = []
        self._lock        = Lock()
        self.set_max_attempt(5) 
        return self

    def get_result_list(self) :
        return self._result_list

    def get_trash_list(self) :
        return self._trash_list

    def set_max_attempt(self, max_attempt) :
        self._max_attempt = max_attempt
        return self

    def set_timeout(self, timeout) :
        self._timeout = timeout
        return self

    def build_threads(self, num_worker_threads) :
        for i in range(num_worker_threads) :
            t = Thread(target = self._work)
            t.daemon = True
            t.start()
        return self

    def add_tasks(self, tasks) :
        _ = lambda task : task['config']
        for task in tasks :
            if task.get('log') is None    : task['log'] = _
            if task.get('result') is None : task['result'] = _
            if task.get('trash') is None  : task['trash'] = _
            self._tasks.put((0, task))
        return self

    def run(self, flog = sys.stdout, is_quiet = True) :
        self._flog = flog
        self._is_quiet = is_quiet
        self._tasks.join()
        return self

    def _log(self, flog, message) :
        flog.write('%s %s\n' % (ctime(), message))
        flog.flush()

    def _curl(self, task) :
        response = request(task['url'](task), timeout = self._timeout)
        if response['e'] is not None : return False
        try :
            content = response['content']
            result  = task['process'](content)
        except Exception, e :
            raise e
        return result

    def _work(self) :
        while True :
            self._lock.acquire()
            num_attempt, task = self._tasks.get()
            self._num_working += 1
            if num_attempt > 0 or not self._is_quiet :
                message = '%d %d %d %d %d %s' % (self._tasks.qsize(), self._num_working, len(self._result_list), len(self._trash_list), num_attempt, task['log'](task))
                self._log(sys.stdout, message)
                if self._flog != sys.stdout :
                    self._log(self._flog, message)
            self._lock.release()
            if num_attempt >= self._max_attempt :
                self._trash_list.append((num_attempt, task['trash'](task)))
            else :
                try :
                    result = self._curl(task)
                except KeyboardInterrupt, e :
                    self._tasks.task_done()
                    self._num_working -= 1
                    raise e
                except Exception, e :
                    self._trash_list.append((-1, task['trash'](task)))
                else :
                    if result is False :
                        self._tasks.put((num_attempt + 1, task))
                    else :
                        self._result_list.append((task['result'](task), result))
            self._tasks.task_done()
            self._num_working -= 1

def task1() :
    mapping   = parse_argv(sys.argv)[0]
    countries = extend(*load_txt(open('../data/country.txt'), is_matrix = True))[ : int(mapping['n'])]
    url       = 'http://www.distancefromto.net/between/%(from)s/%(to)s'
    process   = lambda content : re.findall('([0-9\.]+) km\.', content)[0]
    timing    = time()
    tasks     = []
    for index1 in range(0, len(countries) - 1) :
        for index2 in range(index1 + 1, len(countries)) :
            tasks.append({
                'url'     : lambda task : url % task['config'],
                'process' : process,
                'config'  : {
                    'from' : countries[index1],
                    'to'   : countries[index2],
                }
            })
    flog = open('../log/distance.log', 'w')
    cc = CircularCurler()
    result_list = cc\
        .set_max_attempt(int(mapping['ma']))\
        .build_threads(int(mapping['t']))\
        .add_tasks(tasks)\
        .run(flog = flog, is_quiet = bool(int(mapping['q'])))\
        .get_result_list()
        # .set_timeout(float(mapping['to']))\
    trash_list = cc.get_trash_list()
    fout = open('../data/distance.json', 'w')
    safe_print(fout, j(result_list))
    print '%.2fs' % (time() - timing), 'n = %(n)s t = %(t)s' % mapping
    print len(trash_list), trash_list

def task2() :
    mapping   = parse_argv(sys.argv)[0]
    countries = extend(*load_txt(open('../data/country.txt'), is_matrix = True))
    distances = load_json(open('../data/distance.json'))
    # counter = dict([(country, [country]) for country in countries])
    data = dict([(country, {'COUNTRY' : country}) for country in countries])
    for distance in distances :
        data[distance[0]['from']][distance[0]['to']] = distance[1]
        data[distance[0]['to']][distance[0]['from']] = distance[1]
    data = data.values()
    dump_txt(open('../data/X1.txt', 'w')\
        , sorted(data, key = lambda datum : countries.index(datum['COUNTRY']))\
        , fields = ['COUNTRY'] + countries)
        # counter[distance[0]['from']].append(distance[0]['to'])
        # counter[distance[0]['to']].append(distance[0]['from'])
    # for index, country in enumerate(counter.keys()) :
        # print index, country, set(countries) - set(counter[country])

def task3() :
    mapping   = parse_argv(sys.argv)[0]
    cast = [lambda _ : _] + [lambda _ : '[NONE]' if _ == '' else str(float(re.sub('[^0-9 \-\+\.]', '0', _)))] * 300
    variables = { 
        'X1' : ('M', 0),
        'X2' : ('V', 1),
        'X3' : ('V', 2),
        'X4' : ('V', 1),
        'X5' : ('V', 2),
        'X6' : ('M', 0),
        'X7' : ('M', 0),
        'X8' : ('M', 0),
        'Y02' : ('M', 0),
        'Y03' : ('M', 0),
        'Y04' : ('M', 0),
        'Y05' : ('M', 0),
        'Y06' : ('M', 0),
        'Y07' : ('M', 0),
        'Y08' : ('M', 0),
        'Y09' : ('M', 0),
        'Y10' : ('M', 0),
        'Y11' : ('M', 0),
        'Y12' : ('M', 0),
        'Y13' : ('M', 0),
        'Y14' : ('M', 0),
        'Y15' : ('M', 0),
        'Y16' : ('M', 0),
        'Y17' : ('M', 0),
    }
    data = dict([(variable, load_txt(open('../data/%s.txt' % variable), primary_key = 'COUNTRY', cast = cast)) \
        for variable in variables.keys()])
    result = {}
    for variable, datum in data.items() :
        if variables[variable][0] == 'M' :
            for country1 in datum.keys() :
                for country2 in datum[country1].keys() :
                    if country2 == 'COUNTRY' or country1 == country2: continue
                    key = '%s@%s' % (country1, country2)
                    if result.get(key) is None :
                        result[key] = { 'COUNTRY' : key , 'COUNTRY1' : country1, 'COUNTRY2' : country2 }
                    result[key][variable] = datum[country1][country2]
    for variable, datum in data.items() :
        if variables[variable][0] == 'V' :
            for key in result.keys() :
                if variables[variable][1] == 1 and datum.get(result[key]['COUNTRY1']) is not None :
                    result[key][variable] = datum[result[key]['COUNTRY1']]['X']
                if variables[variable][1] == 2 and datum.get(result[key]['COUNTRY2']) is not None :
                    result[key][variable] = datum[result[key]['COUNTRY2']]['X']
    # print j(result)
    # exit()
    result = result.values()
    dump_txt(open('../data/data.txt', 'w')\
        , sorted(result, key = lambda datum : datum['COUNTRY'])\
        , fields = ['COUNTRY'] + sorted(variables.keys()), default = '[NONE]')

if __name__ == '__main__':
    task3()