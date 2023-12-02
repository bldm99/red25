
import sys
sys.path.append('/app/api')
from api.main import midf
dataframe  = midf
print(type(dataframe))