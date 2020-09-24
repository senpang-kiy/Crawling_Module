from argparse import Namespace

filtering_word = Namespace(
    field_name_quarter_page1=[
        'ROA',
        'ROE',
        'EPS',
        'BPS',
        'DPS',
        'PBR',
        '발행주식수',
        '배당수익율'
    ],
    field_name_quarter_page2=[
        '매출액',
        '매출원가',
        '매출총이익',
        '판매비와관리비',
        '유무형자산상각비',
        '당기순이익',
        '지배주주순이익',
        '비지배주주순이익',
        ##############################################
        '자산',  # 자산총계
        '유동자산',
        '재고자산',
        '매출채권및기타유동채권',
        '부채',  # 부채총계
        '유동부채',
        '자본',  # 자본총계
        '지배기업주주지분',  # 지배주주지분
        ##############################################
        '감가상각비',
        '무형자산상각비'
    ],
    field_name_quarter_page3=[
        '매출액증가율',
        '매출액(-1Y)',
        'EBITDA증가율',
        'EBITDA',
        'EBITDA(-1Y)',
        'EPS증가율',
        'EPS(-1Y)'
    ],

    field_name_page3_5year=[
        '매출액',
        'EBITDA',
        'EPS'
    ],

    field_name_filtering={
        '매출액': '누적매출액',
        'EBITDA': '누적EBITDA',
        'EPS': '누적EPS'
    },

    field_name_change={
        '발행주식수': 'NOSI',  # Number of shares issued
        '배당수익율': 'Dividend_yield',
        '매출액': 'touch',
        '매출원가': 'COS',  # Cost of sales
        '매출총이익': 'Gross_margin',
        '판매비와관리비': 'SAAE',  # Selling and administrative expenses
        '유무형자산상각비': 'AOTAIA',  # Amortization of tangible and intangible assets
        '당기순이익': 'Net_Income',
        '지배주주순이익': 'NPOCS',  # Net profit of controlling shareholder
        '비지배주주순이익': 'NIONS',  # Net income of non-controlling shareholders
        '자산': 'Assets',
        '유동자산': 'Current_assets',
        '재고자산': 'Inventory',
        '매출채권및기타유동채권': 'TRAOCR',  # Trade receivables and other current receivables
        '부채': 'liabilities',
        '유동부채': 'Current_liabilities',
        '자본': 'capital',
        '지배기업주주지분': 'SOTCC',  # Shareholders of the controlling company
        '감가상각비': 'Depreciation',
        '무형자산상각비': 'AOIS',  # Amortization of intangible assets
        '매출액증가율': 'touch_IR',  # touch Increase rate
        '매출액(-1Y)': 'touch_1Y',
        'EBITDA(-1Y)': 'EBITDA_1Y',
        'EPS(-1Y)': 'EPS_1Y',
        'EBITDA증가율': 'EBITDA_IR',  # EVITDA increase rate
        'EPS증가율': 'EPS_IR',
        '누적매출액': 'touch_AML',  # touch accumulate
        '누적EBITDA': 'EBITDA_AML',  # EBITDA accumulate
        '누적EPS': 'EPS_AML'  # EPS_AML
    },

    remove_word=['\xa0']
)

log_config = Namespace(
    filename='D:\\test\\FnGuide_Crawling_log\\',
    level='WARNING',
    rotation="500MB")