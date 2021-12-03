import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

import re

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

def text_to_list(element, delimiter = '|'):
    """
    Receives a string and returns a list of strings
    """
    return element.split(delimiter)

def list_to_dict(element, columns):
    """
    Receives a list of strings and returns a dictionary
    """
    return dict(zip(columns, element))

def treat_date(element):
    """
    Receives a dictionary, creates a new field and returns a dictionary
    """
    element['ano_mes'] = '-'.join(element['data_iniSE'].split('-')[:2])
    return element

def key_uf(element):
    """
    Receives a dictionary and returns a tuple (UF, dictionary)
    """
    key = element['uf']
    return (key, element)

def casos_dengue(element):
    """
    Receives a tuple (UF, [{}, {}])
    returns a tuple ('UF-yyyy-mm', n_casos)
    """
    uf, registers = element
    for register in registers:
        if bool(re.search(r'/d', register['casos'])):
            yield (f"{uf}-{register['ano_mes']}", float(register['casos']))
        else:
            yield (f"{uf}-{register['ano_mes']}", 0.0)

def join_uf_year_month(element):
    """
    receives a list and returns a tuple (UF-date, rain)
    """
    date, mm, uf = element
    date = date[:-3]
    if float(mm) > 0.0:
        return (f"{uf}-{date}", float(mm))
    else:
        return (f'{uf}-{date}', 0.0)

def round_rain(element):
    """
    receives a tuple (UF-date, rain) and returns a tuple (UF-date, rain)
    """
    key, mm = element
    return (key, round(mm, 1))

def filter_empty_fields(element):
    """
    receives a element and returns a tuple (Key, dict)
    """
    key, value = element
    if all([
        value['chuvas'],
        value['dengue'],
        ]):
        return True
    else:
        return False

def uncompact_elements(element):
    """
    receives a element and returns a tuple (Key, dict)
    """
    key, value = element
    rain = value['chuvas'][0]
    dengue = value['dengue'][0]
    uf, year, month = key.split('-')
    return uf, year, month, str(rain), str(dengue)

def prepare_csv(element, delimiter = ';'):
    """
    receives a element and returns a string
    """
    return f"{delimiter}".join(element)

columns_dengue = 'id|data_iniSE|casos|ibge_code|cidade|uf|cep|latitude|longitude'.split('|')

#! Pcollection

print('Casos_dengue')
dengue = (
    pipeline
    | 'Read dataset dengue' >> ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | 'From text to list' >> beam.Map(text_to_list)
    | 'From list to dict' >> beam.Map(lambda element: list_to_dict(element, columns_dengue))
    | 'Create year-month' >> beam.Map(treat_date)
    | 'Create key UF' >> beam.Map(key_uf)
    | 'Group by UF' >> beam.GroupByKey()
    | "Descompact dengue's occurence" >> beam.FlatMap(casos_dengue)
    | 'Sum of cases by key' >> beam.CombinePerKey(sum)
    # | 'Show results dengue' >> beam.Map(print)
)

print('Chuva')
chuvas = (
    pipeline
    | 'Read dataset chuvas' >> ReadFromText('chuvas.csv', skip_header_lines=1)
    | 'From text to list rain' >> beam.Map(text_to_list, delimiter=',')
    | 'Create tuple with key uf-yyyy-mm' >> beam.Map(join_uf_year_month)
    | 'Sum of rain by key' >> beam.CombinePerKey(sum)
    | 'Round rain results' >> beam.Map(round_rain)
    # | 'Show results chuvas' >> beam.Map(print)
)

result = (
    # (chuvas, dengue)
    # | 'Join pcols' >> beam.Flatten()
    # | 'Group pcols' >> beam.CoGroupByKey()
    ({'chuvas' : chuvas, 'dengue' : dengue})
    | 'Merge pcols' >> beam.CoGroupByKey()
    | 'Filter empty fields' >> beam.Filter(filter_empty_fields)
    | 'Uncompact elements' >> beam.Map(uncompact_elements)
    | 'Prepare csv' >> beam.Map(prepare_csv)
    # | 'Show results' >> beam.Map(print)
)

result | 'Create csv file' >> WriteToText(
    'result', 
    file_name_suffix='.csv',
    header = 'UF;YEAR;MONTH;RAIN;DENGUE'
)

pipeline.run()
