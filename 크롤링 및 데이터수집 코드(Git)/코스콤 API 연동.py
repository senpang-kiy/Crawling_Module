


from urllib.request  import Request, urlopen
from urllib.parse  import urlencode, quote_plus
url = 'https://sandbox-apigw.koscom.co.kr/v2/market/multiquote/stocks/{marketcode}/orderbook'.replace('{marketcode}',quote_plus('kospi'))
queryParams = '?' + urlencode({ quote_plus('isuCd') : '005930,000660' ,quote_plus('apikey') : 'l7xx16dca36814414766889a236733a0c18a'  })
request = Request(url + queryParams)
request.get_method = lambda: 'GET'
response_body23 = urlopen(request).read()
print(response_body23)


#%%

from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus
url = 'https://sandbox-apigw.koscom.co.kr/v2/market/stocks/kospi/005930/master'.replace('{marketcode}',quote_plus('kospi')).replace('{issuecode}',quote_plus('001510'))
queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xxc0a9e513672e461b9dd867cb24645b63'  })
request = Request(url + queryParams)
request.get_method = lambda: 'GET'
response_body2 = urlopen(request).read()
print(response_body2)


#%%
import ast
string = response_body.decode("utf-8")
data = ast.literal_eval(string)['result']
data

#%%
A = response_body2
B = A.decode("utf-8")
C = ast.literal_eval(B)['result']
print(A)


