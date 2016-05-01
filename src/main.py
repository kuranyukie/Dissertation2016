# -*- coding: utf-8 -*-  
import sys, os; _paths = filter(lambda _ : _.split('/')[-1] in ['src', 'HelloWord'], [os.path.realpath(__file__ + '/..' * (_ + 1)) for _ in range(os.path.realpath(__file__).count('/'))]); sys.path.extend([_[0] for _ in os.walk(_paths[0])] + [_[0] for _ in os.walk(_paths[1] + '/HelloWord-Lib/src')])
from util import *
from CircularCurler import *

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
    mapping = parse_argv(sys.argv)[0]
    cast = [lambda _ : _] + [lambda _ : '[NONE]' if _ == '' else str(float(re.sub('[^0-9 \-\+\.]', '0', _)))] * 300
    variables = { 
        'X01' : ('M', 0),
        'X02' : ('V', 1),
        'X03' : ('V', 2),
        'X04' : ('V', 1),
        'X05' : ('V', 2),
        'X06' : ('M', 0),
        'X07' : ('M', 0),
        'X08' : ('M', 0),
        'X09' : ('M', 0),
        'X10' : ('M', 0),
        'X11' : ('M', 0),
        'X12' : ('M', 0),
        'X13' : ('M', 0),
        'X14' : ('M', 0),
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
    data = dict([(variable, load_txt(open('../data/X&Y/%s.txt' % variable), primary_key = 'COUNTRY', cast = cast)) \
        for variable in variables.keys()])
    print data['X02']
    exit()
    result = {}
    country_mapping = dict(load_txt(open('../data/country_names.txt'), is_matrix = True))
    for variable, datum in data.items() :
        if variables[variable][0] == 'M' :
            for raw_country1 in datum.keys() :
                country1 = country_mapping.get(raw_country1)
                if country1 is None : continue
                for raw_country2 in datum[raw_country1].keys() :
                    country2 = country_mapping.get(raw_country2)
                    if country2 is None or raw_country2 == 'COUNTRY' or country1 == country2: continue
                    key = '%s@%s' % (country1, country2)
                    if result.get(key) is None :
                        result[key] = {
                            'COUNTRY'      : key ,
                            'COUNTRY1'     : country1,
                            'COUNTRY2'     : country2,
                            'RAW_COUNTRY1' : raw_country1,
                            'RAW_COUNTRY2' : raw_country2,
                        }
                    result[key][variable] = datum[raw_country1][raw_country2]
    for variable, datum in data.items() :
        if variables[variable][0] == 'V' :
            for key in result.keys() :
                if variables[variable][1] == 1 :
                    if datum.get(result[key]['COUNTRY1']) is not None :
                        country = result[key]['COUNTRY1']
                    elif datum.get(result[key]['RAW_COUNTRY1']) is not None :
                        country = result[key]['RAW_COUNTRY1']
                    else :
                        # print result[key]['COUNTRY1'], result[key]['RAW_COUNTRY1'], variable
                        continue
                    result[key][variable] = datum[country]['X']
                if variables[variable][1] == 2 :
                    if datum.get(result[key]['COUNTRY2']) is not None :
                        country = result[key]['COUNTRY2']
                    elif datum.get(result[key]['RAW_COUNTRY2']) is not None :
                        country = result[key]['RAW_COUNTRY2']
                    else :
                        # print result[key]['COUNTRY2'], result[key]['RAW_COUNTRY2'], variable
                        continue
                    result[key][variable] = datum[country]['X']
    xs = ['X01', 'X02', 'X03', 'X04', 'X05', 'X06', 'X07', 'X08', 'X09', 'X10', 'X11', 'X12', 'X13', 'X14']
    ys = ['Y02', 'Y03', 'Y04', 'Y05', 'Y06', 'Y07', 'Y08', 'Y09', 'Y10', 'Y11', 'Y12', 'Y13', 'Y14', 'Y15', 'Y16', 'Y17']
    for key in result.keys() :
        x_values = filter(lambda _ : _ not in [ None, '[NONE]' ], map(result[key].get, xs))
        y_values = filter(lambda _ : _ not in [ None, '[NONE]' ], map(result[key].get, ys))
        if len(x_values) < len(xs) or len(y_values) == 0 : result.pop(key)
        else :
            y_values = map(float, y_values)
            result[key]['Y'] = str(calc_mean(y_values))
    result = result.values()
    dump_txt(open('../data/data.txt', 'w')\
        , sorted(result, key = lambda datum : datum['COUNTRY'])\
        , fields = ['COUNTRY'] + ['Y'] + xs, default = '[NONE]')

def task4() :
    country_mapping = dict(load_txt(open('../data/country_names.txt'), is_matrix = True))
    data = []
    ignore_words = set(['South', 'of', 'Islands', 'United', 'and', 'Asia', 'Western', 'Southern', 'Eastern', 'Northern', 'Central', 'Territories', 'Saint', 'St', 'Republic'])
    for country1, country2 in country_mapping.items() :
        if country1 == country2 :
            data.append((country1, set(re.split('[^a-zA-Z]+', country1))))
            for country, words in data[:-1] :
                if len(words.intersection(data[-1][1]) - ignore_words) > 0 :
                    print country1, '@', country
    # print j(country_mapping.keys())

def task5() :
    mapping   = parse_argv(sys.argv)[0]
    countries = ["albania", "angola", "argentina", "australia", "austria", "bangladesh", "belgium", "bhutan", "brazil", "bulgaria", "Burkina_Faso", "canada", "Cape_Verde", "chile", "china", "colombia", "costa-rica", "croatia", "czech-republic", "denmark", "Dominican_Republic", "ecuador", "egypt", "el-salvador", "estonia", "ethiopia", "Fiji", "finland", "france", "germany", "ghana", "greece", "guatemala", "Honduras", "hong-kong", "hungary", "iceland", "india", "indonesia", "iran", "iraq", "ireland", "israel", "italy", "jamaica", "japan", "jordan", "kenya", "kuwait", "latvia", "lebanon", "libya", "lithuania", "luxemburg", "malawi", "malaysia", "malta", "mexico", "morocco", "mozambique", "Namibia", "nepal", "netherlands", "new-zealand", "nigeria", "norway", "pakistan", "panama", "peru", "philippines", "poland", "portugal", "romania", "russia", "saudi-arabia", "senegal", "serbia", "sierra-leone", "singapore", "slovakia", "slovenia", "south-africa", "south-korea", "spain", "sri_lanka", "surinam", "sweden", "switzerland", "syria", "taiwan", "tanzania", "thailand", "trinidad", "turkey", "ukraine", "arab-emirates", "united-kingdom", "united-states", "uruguay", "venezuela", "vietnam", "zambia"]
    countries = countries[ : int(mapping['n'])]
    url       = 'https://geert-hofstede.com/%(country)s.html'
    process   = lambda content : dict(re.findall('([a-z]+):(\d+)', re.sub('[ \t\n]', '', re.findall('var data = (\{[^}]+\})', content)[0])))
    timing    = time()
    tasks     = []

    for country in countries :
        tasks.append({
            'url'     : lambda task : url % task['config'],
            'process' : process,
            'config'  : {
                'country' : country,
            }
        })
    flog = open('../log/hofstede.log', 'w')
    cc = CircularCurler()
    result_list = cc\
        .set_max_attempt(int(mapping['ma']))\
        .build_threads(int(mapping['t']))\
        .set_timeout(float(mapping['to']))\
        .add_tasks(tasks)\
        .run(flog = flog, is_quiet = bool(int(mapping['q'])))\
        .get_result_list()
    trash_list = cc.get_trash_list()
    fout = open('../data/hofstede.json', 'w')
    safe_print(fout, j(result_list))
    print '%.2fs' % (time() - timing), 'n = %(n)s t = %(t)s' % mapping
    print len(trash_list), trash_list

def task6() :
    data = load_json(open('../data/Xs/hofstede.json'))
    country_mapping = dict(load_txt(open('../data/country_names.txt'), is_matrix = True))
    fields = {
        'idv' : 'X09',
        'ind' : 'X10',
        'lto' : 'X11',
        'mas' : 'X12',
        'pdi' : 'X13',
        'uai' : 'X14'}
    for field in fields.keys() :
        result = {}
        for index1 in range(0, len(data)) :
            country1 = country_mapping.get(data[index1][0]['country'])
            # country1 = data[index1][0]['country']
            if country1 is None :
                print data[index1][0]['country']
                continue
            result[country1] = { 'COUNTRY' : country1 }
            for index2 in range(0, len(data)) :
                # if index1 == index2 : continue
                country2 = country_mapping.get(data[index2][0]['country'])
                # country2 = data[index2][0]['country']
                if country2 is None :
                    print data[index2][0]['country']
                    continue
                index = '%s@%s' % (country1, country2)
                if not data[index1][1].has_key(field) or not data[index2][1].has_key(field) :
                    result[country1][country2] = '1.00'
                else :
                    _ = 1.0 * int(data[index1][1][field]) / int(data[index2][1][field])
                    result[country1][country2] = '%.2f' % _
        dump_txt(open('../data/X&Y/%s.txt' % fields[field], 'w')\
            , sorted(result.values(), key = lambda datum : datum['COUNTRY'])\
            , fields = ['COUNTRY'] + sorted([datum['COUNTRY'] for datum in result.values()])\
            , default = '[NONE]')

if __name__ == '__main__':
    # python main.py -n=5 -ma=3 -t=10 -q=0
    task6()
