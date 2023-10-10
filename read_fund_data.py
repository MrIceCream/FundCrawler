import openpyxl

def read(file, is_enhance):
    # 读取Excel文件
    workbook = openpyxl.load_workbook(file, data_only=True)
    sheet = workbook.active
    result = []
    for row in sheet.iter_rows():
        if len(row[0].value) != 6:
            continue
        if is_enhance and '增强' in str(row[1].value) or \
                not is_enhance and '增强' not in str(row[1].value):
            result.append({'code':row[0].value,'name':row[1].value})
    return result

def read_gem(file, is_enhance):
    # 读取Excel文件
    workbook = openpyxl.load_workbook(file, data_only=True)
    sheet = workbook.active
    result = []
    for row in sheet.iter_rows():
        if len(row[7].value) != 6 or '境外' in row[5].value:
            continue
        if is_enhance and '增强' in str(row[1].value) or \
                not is_enhance and '增强' not in str(row[1].value):
            result.append({'code':row[7].value,'name':row[1].value})
    return result

def get_fund_list(is_enhance):
    sz50 = read('source/上证50.xlsx', is_enhance)
    hs300 = read('source/沪深300.xlsx', is_enhance)
    zz500 = read('source/中证500.xlsx', is_enhance)
    zz1000 = read('source/中证1000.xlsx', is_enhance)
    cyb = read_gem('source/创业板指.xlsx', is_enhance)
    return sz50 + hs300 + zz500 + zz1000 + cyb