from pydantic import (
    BaseModel as PydanticBaseModel, 
    validator, 
	constr
)

import pendulum

try:
    # Only available on Python >=3.8
    from typing import Literal
except ImportError:
    # Available on Python >=3.7, and the docker we're using is on Python 3.7
    from typing_extensions import Literal

# These are for CoinDesk's Bitcoin Price Index API (currentprice.json)
# https://api.coindesk.com/v1/bpi/currentprice.json
class BaseModel(PydanticBaseModel):
    class Config:
        extra = 'allow'

class BPITime(BaseModel):
    updated: str
    updatedISO: str
        
    @validator('updated')
    def updated_match_format(cls, value):
        assert pendulum.from_format(value, 'MMM DD, YYYY HH:mm:ss z'), "updated doesn't match its predefined format"
        return value
    
    @validator('updatedISO')
    def updatedISO_match_format(cls, value):
        assert pendulum.from_format(value, 'YYYY-MM-DDTHH:mm:ssZ'), "updatedISO doesn't match its predefined format"
        return value

class BPICurrencyRate(BaseModel):
    code: constr(min_length = 3, max_length = 3, strip_whitespace = True)
    description: str
    rate_float: float
        
class BPICurrency(BaseModel):
    USD: BPICurrencyRate
    GBP: BPICurrencyRate
    EUR: BPICurrencyRate
    
class BPI(BaseModel):
    time: BPITime
    disclaimer: str
    chartName: Literal['Bitcoin']
    bpi: BPICurrency

# These are for Open Exchange Rates' historical rates API
# https://docs.openexchangerates.org/reference/historical-json
# (might work for latest rates API too, as far their formats remain compatible)       
class CurrencyXR(BaseModel):
    # For our purpose, only IDR is required to exist, the rest is optional
    # The API actually allows fetching all currencies at once, not just IDR
    IDR: float
        
class HistoricalXR(BaseModel):
    disclaimer: str
    license: Literal['https://openexchangerates.org/license']
    timestamp: int
    base: constr(min_length = 3, max_length = 3, strip_whitespace = True)
    rates: CurrencyXR
	    
    @validator('timestamp')
    def UNIX_timestamp_match(cls, value):
        assert pendulum.from_timestamp(value), "timestamp isn't a UNIX timestamp!"
        return value